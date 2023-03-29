package graphqlws

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	fasthttpWebsocket "github.com/fasthttp/websocket"
	"github.com/fraym/golog"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/graphql-go/graphql"
)

type ConnectionParams struct{}

type Connection struct {
	mx     sync.RWMutex
	config *Config
	ws     *websocket.Conn
	schema *graphql.Schema
	logger golog.Logger

	isInit     bool
	isEnded    bool
	operations map[string]Subscription
	params     map[string]any
}

func NewConnection(config *Config, ws *websocket.Conn, schema *graphql.Schema, logger golog.Logger) *Connection {
	return &Connection{
		mx:         sync.RWMutex{},
		config:     config,
		ws:         ws,
		schema:     schema,
		logger:     logger,
		isInit:     false,
		isEnded:    false,
		operations: map[string]Subscription{},
		params:     map[string]any{},
	}
}

func (c *Connection) Run() {
	defer c.Close()

	if err := c.runReceiver(); err != nil {
		if fasthttpWebsocket.IsUnexpectedCloseError(err, fasthttpWebsocket.CloseNormalClosure, fasthttpWebsocket.CloseGoingAway) {
			c.logger.Error().WithError(err).Write()
		}
	}
}

func (c *Connection) Close() {
	c.mx.Lock()
	defer c.mx.Unlock()

	c.isEnded = true
	c.ws.Close()
}

func (c *Connection) runReceiver() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	initCtx, initDone := context.WithTimeout(context.Background(), c.config.ConnectionInitWaitTimeout)
	defer initDone()

	pinger := NewPinger(ctx, cancel, c.config.PingInterval, c.config.PingTimeout, c.logger, func() error {
		return c.sendPing()
	})

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-initCtx.Done():
			if c.isInitialized() {
				return
			}

			_ = c.sendInitTimeout()
			return
		}
	}()

	for {
		_, msg, err := c.ws.ReadMessage()
		if err != nil {
			return err
		}

		var data map[string]any
		if err := json.Unmarshal(msg, &data); err != nil {
			return err
		}

		t, ok := data["type"].(string)
		if !ok {
			return fmt.Errorf("graphql-ws: missing `type` in data")
		}

		switch t {
		case "ping":
			if err := c.onPing(); err != nil {
				return err
			}
		case "pong":
			pinger.OnPong()
		case "connection_init":
			if err := c.onInit(initDone, data); err != nil {
				return err
			}
		case "subscribe":
			if err := c.onSubscribe(ctx, data); err != nil {
				return err
			}
		case "complete":
			if err := c.onComplete(data); err != nil {
				return err
			}
		default:
			return c.sendInvalidType(t)
		}
	}
}

func (c *Connection) onInit(initDone context.CancelFunc, data map[string]any) error {
	if c.isInitialized() {
		return c.sendTooManyInitRequests()
	}

	c.mx.Lock()
	payload, ok := data["payload"].(map[string]any)
	if ok {
		c.params = payload
	}

	c.isInit = true
	c.mx.Unlock()

	initDone()

	return c.sendConnectionAck()
}

func (c *Connection) onSubscribe(ctx context.Context, data map[string]any) error {
	if !c.isInitialized() {
		return c.sendUnauthorized()
	}

	id, ok := data["id"].(string)
	if !ok {
		return fmt.Errorf("data does not contain a valid `id` field")
	}

	c.mx.Lock()
	defer c.mx.Unlock()

	if _, ok := c.operations[id]; ok {
		return c.sendSubscriberAlreadyExists(id)
	}

	payload, ok := data["payload"].(map[string]any)
	if !ok {
		return fmt.Errorf("data does not contain a valid `payload` field")
	}

	subscription, err := NewSubscription(ctx, c.schema, id, payload, c.params)
	if err != nil {
		return err
	}

	c.operations[id] = *subscription

	go subscription.Run(c.sendResult, c.sendComplete, c.logger)

	return nil
}

func (c *Connection) onComplete(data map[string]any) error {
	id, ok := data["id"].(string)
	if !ok {
		return fmt.Errorf("data does not contain a valid `id` field")
	}

	c.mx.Lock()
	defer c.mx.Unlock()

	delete(c.operations, id)

	return nil
}

func (c *Connection) isInitialized() bool {
	c.mx.RLock()
	defer c.mx.RUnlock()

	return c.isInit
}

func (c *Connection) isClosed() bool {
	c.mx.RLock()
	defer c.mx.RUnlock()

	return c.isEnded
}

func (c *Connection) onPing() error {
	if c.isClosed() {
		return nil
	}

	return c.ws.WriteJSON(fiber.Map{
		"type": "pong",
	})
}

func (c *Connection) sendResult(id string, result *graphql.Result) error {
	if c.isClosed() {
		return nil
	}

	if result.HasErrors() {
		return c.ws.WriteJSON(fiber.Map{
			"id":      id,
			"type":    "error",
			"payload": result.Errors,
		})
	}

	return c.ws.WriteJSON(fiber.Map{
		"id":      id,
		"type":    "next",
		"payload": result.Data,
	})
}

func (c *Connection) sendComplete(id string) error {
	if c.isClosed() {
		return nil
	}

	return c.ws.WriteJSON(fiber.Map{
		"id":   id,
		"type": "complete",
	})
}

func (c *Connection) sendPing() error {
	if c.isClosed() {
		return nil
	}

	return c.ws.WriteJSON(fiber.Map{
		"type": "ping",
	})
}

func (c *Connection) sendConnectionAck() error {
	if c.isClosed() {
		return nil
	}

	return c.ws.WriteJSON(fiber.Map{
		"type": "connection_ack",
	})
}

func (c *Connection) sendInitTimeout() error {
	if c.isClosed() {
		return nil
	}

	return c.ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
		4408,
		"Connection initialisation timeout",
	))
}

func (c *Connection) sendTooManyInitRequests() error {
	if c.isClosed() {
		return nil
	}

	return c.ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
		4429,
		"Too many initialisation requests",
	))
}

func (c *Connection) sendUnauthorized() error {
	if c.isClosed() {
		return nil
	}

	return c.ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
		4401,
		"Unauthorized",
	))
}

func (c *Connection) sendSubscriberAlreadyExists(id string) error {
	if c.isClosed() {
		return nil
	}

	return c.ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
		4409,
		fmt.Sprintf("Subscriber for %s already exists", id),
	))
}

func (c *Connection) sendInvalidType(t string) error {
	if c.isClosed() {
		return nil
	}

	c.logger.Error().Writef("unknown message type %s", t)
	return c.ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
		4400,
		fmt.Sprintf("unknown message type %s", t),
	))
}

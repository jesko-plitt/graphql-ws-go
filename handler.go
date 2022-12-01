package graphqlws

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/graphql-go/graphql"
)

type SubscriptionHandler struct {
	config *Config
	logger Logger
}

func NewSubscriptionHandler(
	logger Logger,
	config *Config,
) *SubscriptionHandler {
	if config == nil {
		config = GetDefaultConfig()
	}

	return &SubscriptionHandler{
		config: config,
		logger: logger,
	}
}

func (h *SubscriptionHandler) Handle(c *fiber.Ctx, schema *graphql.Schema) error {
	return websocket.New(func(c *websocket.Conn) {
		conn := NewConnection(h.config, c, schema, h.logger)
		conn.Run()
	}, websocket.Config{
		Subprotocols: []string{"graphql-transport-ws"},
	})(c)
}

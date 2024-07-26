package graphqlws

import (
	"github.com/fraym/golog"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/graphql-go/graphql"
)

type SubscriptionHandler struct {
	config *Config
	logger golog.Logger
}

func NewSubscriptionHandler(
	logger golog.Logger,
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

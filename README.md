# graphql-ws-go

Implementation of the [graphql-ws protocoll](https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md) in go. It uses the subprotocol `graphql-transport-ws`. This library is currently designed for use with [fiber](https://github.com/gofiber/fiber).

## Usage

### Instantiate the handler

```go
handler := graphqlws.NewSubscriptionHandler(logger, &graphqlws.Config{
	ConnectionInitWaitTimeout: time.Second,
	PingInterval:              2 * time.Second,
	PingTimeout:               5 * time.Second,
})
```

You can pass `nil` instead ot the `graphqlws.Config` struct reference. In this case the handler falls back to the default config provided by `graphqlws.GetDefaultConfig()`.

### Use the fiber handler

```go
var ExampleType = graphql.NewObject(graphql.ObjectConfig{
	Name: "ExampleType",
	Fields: graphql.Fields{
		"value": &graphql.Field{
			Type: graphql.String,
		},
	},
})

schema, err := graphql.NewSchema(graphql.SchemaConfig{
	// ...
	Subscription: graphql.NewObject(graphql.ObjectConfig{
		Name:        "Subscription",
		Description: "The application's root subscription object",
		Fields: graphql.Fields{
			"example": &graphql.Field{
				Type: ExampleType,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return p.Source, nil
				},
				Subscribe: func(p graphql.ResolveParams) (interface{}, error) {
					c := make(chan interface{})
					// you can send structs or maps to c that json.Marshal to a structure that matches the type
					// (in this case "ExampleType")

					return c, nil
				},
			},
		},
	}),
})
app := fiber.New()

app.Get("/subscription", func(c *fiber.Ctx) error {
	return handler.Handle(c, &schema)
})
```

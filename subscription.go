package graphqlws

import (
	"context"
	"fmt"

	"github.com/fraym/golog"
	graphql "github.com/fraym/graphql-go"
)

type Subscription struct {
	ctx       context.Context
	cancel    context.CancelFunc
	id        string
	operation *graphql.Params
}

func NewSubscription(
	ctx context.Context,
	schema *graphql.Schema,
	id string,
	payload map[string]any,
	params map[string]any,
) (*Subscription, error) {
	subscriptionCtx := context.WithValue(ctx, ConnectionParams{}, params)
	operation, err := getSubscriptionOperation(subscriptionCtx, schema, payload)
	if err != nil {
		return nil, err
	}

	subscriptionCtxWithCancel, cancelSubscription := context.WithCancel(subscriptionCtx)

	return &Subscription{
		ctx:       subscriptionCtxWithCancel,
		cancel:    cancelSubscription,
		id:        id,
		operation: operation,
	}, nil
}

type (
	SendComplete func(id string) error
	SendResult   func(id string, result *graphql.Result) error
)

func (s *Subscription) Run(sendResult SendResult, sendComplete SendComplete, logger golog.Logger) {
	resultChannel := graphql.Subscribe(*s.operation)

	for {
		select {
		case <-s.ctx.Done():
			return
		case result, ok := <-resultChannel:
			if !ok {
				if err := sendComplete(s.id); err != nil {
					logger.Error().WithError(err).Write()
				}
				return
			}

			if err := sendResult(s.id, result); err != nil {
				logger.Error().WithError(err).Write()
				return
			}
		}
	}
}

func (s *Subscription) End() {
	s.cancel()
}

func getSubscriptionOperation(
	ctx context.Context,
	schema *graphql.Schema,
	payload map[string]any,
) (*graphql.Params, error) {
	variables := map[string]any{}

	if newVariables, ok := payload["variables"].(map[string]any); ok {
		variables = newVariables
	}

	operationName := ""
	if payload["operationName"] != nil {
		operationName = payload["operationName"].(string)
	}

	query, ok := payload["query"].(string)
	if !ok {
		return nil, fmt.Errorf("payload does not contain a valid `query` field")
	}

	return &graphql.Params{
		Context:        ctx,
		Schema:         *schema,
		VariableValues: variables,
		OperationName:  operationName,
		RequestString:  query,
	}, nil
}

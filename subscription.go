package graphqlws

import (
	"context"
	"fmt"

	"github.com/graphql-go/graphql"
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
) (*Subscription, error) {
	operation, err := getSubscriptionOperation(ctx, schema, payload)
	if err != nil {
		return nil, err
	}

	subscriptionCtx, cancelSubscription := context.WithCancel(ctx)

	return &Subscription{
		ctx:       subscriptionCtx,
		cancel:    cancelSubscription,
		id:        id,
		operation: operation,
	}, nil
}

type SendComplete func(id string) error
type SendResult func(id string, result *graphql.Result) error

func (s *Subscription) Run(sendResult SendResult, sendComplete SendComplete, logger Logger) {
	resultChannel := graphql.Subscribe(*s.operation)

	for {
		select {
		case <-s.ctx.Done():
			return
		case result, ok := <-resultChannel:
			if !ok {
				if err := sendComplete(s.id); err != nil {
					logger.Error(err)
				}
				return
			}

			if err := sendResult(s.id, result); err != nil {
				logger.Error(err)
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

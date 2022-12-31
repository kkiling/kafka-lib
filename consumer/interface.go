package consumer

import (
	"context"
)

type Message struct {
	Topic string
	Value []byte
	Key   []byte
}

type MessageError struct {
	Msg   *Message
	Error error
}

type IConsumer interface {
	ProcessMessages(ctx context.Context) error
	AddHandler(ctx context.Context, countHandlers int, handler func(ctx context.Context, msg Message) error)
	Shutdown() error
	SubscribeTopics() error
	Unsubscribe() error
}

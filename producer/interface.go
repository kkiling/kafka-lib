package producer

import (
	"context"
)

type IProducer interface {
	ProduceMsg(ctx context.Context, topic string, key, msg interface{}) error
	Shutdown() error
}

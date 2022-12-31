package producer_impl

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"strings"
)

type Cfg struct {
	BootstrapServers []string `yaml:"bootstrap_servers"`
	FlushTimeoutMs   int      `yaml:"flush_timeout_ms" default:"5000"`
}

type Producer struct {
	flushTimeoutMs int
	producer       *kafka.Producer
}

func NewProducer(cfg Cfg) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":            strings.Join(cfg.BootstrapServers, ","),
		"acks":                         "all",
		"queue.buffering.max.kbytes":   1024 * 1024,
		"queue.buffering.max.messages": 1000000,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Producer{
		producer:       p,
		flushTimeoutMs: cfg.FlushTimeoutMs,
	}, nil
}

func (k *Producer) ProduceMsg(ctx context.Context, topic string, key, msg interface{}) error {
	keyData, err := json.Marshal(key)
	if err != nil {
		return errors.Wrap(err, "fail marshal key")
	}

	msgData, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "fail marshal msg")
	}

	err = k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            keyData,
		Value:          msgData,
	}, nil)

	if kafkaError, ok := err.(kafka.Error); ok && kafkaError.Code() == kafka.ErrQueueFull {
		log.Warn().Err(err).Msg("kafka local queue full error - Going to Flush then retry...")
		flushedMessages := k.producer.Flush(k.flushTimeoutMs)
		log.Info().Msgf("flushed kafka messages. Outstanding events still un-flushed: %d", flushedMessages)
		return k.ProduceMsg(ctx, topic, key, msg)
	}

	if err != nil {
		return fmt.Errorf("failed to produce message with error: %w", err)
	}

	return nil
}

func (k *Producer) Shutdown() error {
	// Wait for all messages to be delivered
	k.producer.Flush(k.flushTimeoutMs)
	k.producer.Close()
	return nil
}

package consumer_impl

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/kkiling/kafka-lib/consumer"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"strings"
	"sync"
	"time"
)

type message struct {
	Topic string
	Value []byte
	Key   []byte
	Try   int
}

type result struct {
	Ok    bool
	Error error
	Try   int
	Msg   *message
}

type Cfg struct {
	BootstrapServers []string `yaml:"bootstrap_servers"`
	GroupId          string   `yaml:"group_id"`
	Topics           []string `yaml:"topics"`
	// Максимальное количество сообщений находящихся в работе
	MaxCountMsgInProcessing int `yaml:"max_count_msg_in_processing" default:"100"`
	// Максимальное количество попыток повторной обработки сообщений, прежде чем вызовется паника
	MaxCountReprocessingMsg int `yaml:"max_reprocessing_msg" default:"3"`
	// Таймаут повторной обработки сообщений в миллисекундах
	TimeoutReprocessingMsg int `yaml:"timeout_reprocessing_msg" default:"1"`
}

type KafkaConsumer struct {
	consumer                  *kafka.Consumer
	cfg                       Cfg
	newMessageChan            chan message
	resultChan                chan result
	processing                bool
	countMessagesInProcessing int // Количество сообщений в обработке
	currentOffset             map[string]kafka.TopicPartition
	// Канал, в который отправляется сообщение с ошибкой, которую не удалось обработать
	// Если этот канал не задан, информация об ошибке выводиться в log.Fatal
	messageErrorChan chan<- consumer.MessageError
}

func NewConsumer(cfg Cfg, messageErrorChan chan<- consumer.MessageError) (*KafkaConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(cfg.BootstrapServers, ","),
		"group.id":                 cfg.GroupId,
		"go.events.channel.enable": true,
		"enable.auto.commit":       false,
		"go.events.channel.size":   cfg.MaxCountMsgInProcessing,
		"auto.offset.reset":        "earliest",
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &KafkaConsumer{consumer: c,
		cfg:                       cfg,
		newMessageChan:            make(chan message, 100),
		resultChan:                make(chan result, 100),
		countMessagesInProcessing: 0,
		processing:                true,
		currentOffset:             make(map[string]kafka.TopicPartition, 0),
		messageErrorChan:          messageErrorChan,
	}, nil
}

func (k *KafkaConsumer) commit() error {
	currentOffsets := make([]kafka.TopicPartition, 0, len(k.currentOffset))
	for _, offset := range k.currentOffset {
		offset.Offset++
		currentOffsets = append(currentOffsets, offset)
	}

	log.Debug().Msg("start commit")
	tps, err := k.consumer.CommitOffsets(currentOffsets)

	if ke, ok := err.(kafka.Error); ok {
		if ke.Code() == kafka.ErrNoOffset {
			log.Debug().Err(err).Msgf("error no offset")
		} else {
			return err
		}
	} else if err != nil {
		return err
	}

	for _, tp := range tps {
		log.Debug().Msgf("commit %s: %d", *tp.Topic, tp.Offset)
	}

	if !k.processing {
		k.processing = true
		log.Debug().Msg("start processing message")
	}

	return nil
}

func (k *KafkaConsumer) onError(res result) {
	if k.messageErrorChan == nil {
		log.Fatal().Err(res.Error).Msgf("message error result")
	} else {
		k.messageErrorChan <- consumer.MessageError{
			Msg: &consumer.Message{
				Topic: res.Msg.Topic,
				Value: res.Msg.Value,
				Key:   res.Msg.Key,
			},
			Error: res.Error,
		}
	}
}

func (k *KafkaConsumer) onResultError(newMessageChan chan<- message, res result) bool {
	if err, ok := res.Error.(*consumer.ProcessedMsgError); ok {
		if err.Type == consumer.PanicErrorType || res.Try >= k.cfg.MaxCountReprocessingMsg {
			k.onError(res)
			return true
		} else if err.Type == consumer.CanTryToFixErrorType {
			// Отправляем сообщение на обработку повторно по таймауту
			time.Sleep(time.Duration(k.cfg.TimeoutReprocessingMsg) * time.Second)
			newMessageChan <- message{
				Topic: res.Msg.Topic,
				Key:   res.Msg.Key,
				Value: res.Msg.Value,
				Try:   res.Try + 1,
			}
			log.Info().Msgf("try %d reprocessing msg", res.Try)
			return false
		} else if err.Type == consumer.InfoErrorType {
			log.Error().Err(res.Error).Str("key", string(res.Msg.Key)).Msgf("info consumer error")
			return true
		}
	}

	k.onError(res)
	return true
}

func (k *KafkaConsumer) resultChanHandler(ctx context.Context, newMessageChan chan<- message, resultChan <-chan result) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case res, isOpen := <-resultChan:
			if !isOpen {
				return
			}

			if res.Ok {
				k.countMessagesInProcessing--
			} else {
				log.Debug().Err(res.Error).Str("key", string(res.Msg.Key)).Msgf("result message error")
				if k.onResultError(newMessageChan, res) {
					k.countMessagesInProcessing--
				}
			}

			if ctx.Err() != nil {
				return
			}

			if k.countMessagesInProcessing <= 0 {
				if err := k.commit(); err != nil {
					log.Fatal().Err(err).Msgf("fail commit messages")
				}
			}
		}
	}
}

func (k *KafkaConsumer) eventChanHandler(ctx context.Context, newMessageChan chan<- message) error {
	for ctx.Err() == nil {
		if !k.processing {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, isOpen := <-k.consumer.Events():
			if !isOpen {
				return nil
			}
			switch newMessage := ev.(type) {
			case *kafka.Message:
				k.countMessagesInProcessing++
				log.Debug().Msgf("consume message - %s: %d",
					*newMessage.TopicPartition.Topic, newMessage.TopicPartition.Offset)
				k.currentOffset[*newMessage.TopicPartition.Topic] = newMessage.TopicPartition

				newMessageChan <- message{
					Topic: *newMessage.TopicPartition.Topic,
					Key:   newMessage.Key,
					Value: newMessage.Value,
					Try:   0,
				}

				if k.countMessagesInProcessing >= k.cfg.MaxCountMsgInProcessing {
					k.processing = false
					log.Debug().Msgf("stop processing message")
				}

			case kafka.Error:
				log.Error().Err(newMessage).Msgf("kafka message error")
			}
		}
	}

	return ctx.Err()
}

func (k *KafkaConsumer) ProcessMessages(ctx context.Context) error {
	go k.resultChanHandler(ctx, k.newMessageChan, k.resultChan)
	return k.eventChanHandler(ctx, k.newMessageChan)
}

func (k *KafkaConsumer) SubscribeTopics() error {
	err := k.consumer.SubscribeTopics(k.cfg.Topics, nil)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}
	return nil
}

func (k *KafkaConsumer) Unsubscribe() error {
	err := k.consumer.Unsubscribe()
	if err != nil {
		return errors.Wrap(err, "failed to unsubscribe from topics")
	}
	return nil
}

func (k *KafkaConsumer) AddHandler(ctx context.Context, countHandlers int,
	handler func(ctx context.Context, msg consumer.Message) error) {
	wg := sync.WaitGroup{}
	for wgIndex := 1; wgIndex <= countHandlers; wgIndex++ {
		wg.Add(1)
		go func(wgIndex int) {
			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return
				case msg, isOpen := <-k.newMessageChan:
					if !isOpen {
						return
					}

					err := handler(ctx, consumer.Message{
						Topic: msg.Topic,
						Value: msg.Value,
						Key:   msg.Key,
					})

					if ctx.Err() != nil {
						return
					}

					if err != nil {
						k.resultChan <- result{
							Ok:    false,
							Error: err,
							Msg:   &msg,
							Try:   msg.Try,
						}
					} else {
						k.resultChan <- result{
							Ok:  true,
							Msg: &msg,
							Try: msg.Try,
						}
					}
				}
			}
		}(wgIndex)
	}
	wg.Wait()
}

func (k *KafkaConsumer) Shutdown() error {
	defer close(k.newMessageChan)
	defer close(k.resultChan)
	return k.consumer.Close()
}

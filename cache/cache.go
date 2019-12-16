package cache

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	"github.com/jollyburger/xutils/kafka"
	"go.uber.org/zap"
)

type CacheHandler interface {
	Refresh(input interface{}) (interface{}, string, []byte, error)
}

type CacheModel struct {
	logger   *zap.Logger
	redisCli *redis.Client

	handlers map[string]CacheHandler
}

func NewCacheModel(logger *zap.Logger, redisCli *redis.Client) (*CacheModel, error) {
	cm := new(CacheModel)
	cm.logger = logger
	cm.redisCli = redisCli

	cm.handlers = make(map[string]CacheHandler)

	return cm, nil
}

func (this *CacheModel) GetCacheData(key string) (string, error) {
	return this.redisCli.Get(key).Result()
}

func (this *CacheModel) RefreshByCallback(refresh bool, in interface{}, callback func(interface{}) (interface{}, string, []byte, error)) (interface{}, error) {
	var (
		data      interface{}
		key       string
		cacheData []byte
		err       error
	)

	if refresh {
		data, key, cacheData, err = callback(in)
	}

	if err == nil {
		go this.setCacheData(key, cacheData)
	}

	if err == CacheEmptyError {
		return data, nil
	}

	return data, err
}

func (this *CacheModel) setCacheData(key string, data interface{}) error {
	_, err := this.redisCli.Set(key, data.([]byte), 0).Result()
	if err != nil {
		this.logger.Error("set cache data error",
			zap.String("key", key),
			zap.Error(err))
	}
	return err
}

func (this *CacheModel) RegisterHandler(name string, handler CacheHandler) {
	if _, ok := this.handlers[name]; !ok {
		this.handlers[name] = handler
	}
}

func (this *CacheModel) RunCacheTask(refresh bool, in interface{}, handlerName string) (interface{}, error) {
	if handler, ok := this.handlers[handlerName]; ok {
		return this.RefreshByCallback(refresh, in, handler.Refresh)
	}

	return nil, CacheHandlerNotFound
}

//kafka operation
func (this *CacheModel) PushCacheTask(refresh bool, in interface{}, handlerName string) {
	kafkaConfig := kafka.GetKafkaConfig()

	kafkaPublisher, err := kafka.NewKafkaProducer(kafkaConfig.Servers, this.logger)
	if err != nil {
		this.logger.Error("get kafka publisher instance error",
			zap.Any("servers", kafkaConfig.Servers),
			zap.Error(err))

		return
	}

	var body = map[string]interface{}{
		"refresh": refresh,
		"data":    in,
		"name":    handlerName,
	}

	//build kafka msg
	buf, _ := json.Marshal(body)
	msg := new(sarama.ProducerMessage)
	msg.Key = sarama.StringEncoder(fmt.Sprintf("%d", time.Now().UnixNano()))
	msg.Value = sarama.StringEncoder(buf)
	msg.Topic = kafkaConfig.Topic

	kafkaPublisher.Input() <- msg
}

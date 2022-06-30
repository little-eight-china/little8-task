package scheduler

import (
	"schedule-test/lib/log"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"schedule-test/task"
)

type Subscriber struct {
	*zap.Logger

	*task.DefaultPeriodTask
	task.Runner

	consumerGroup sarama.ConsumerGroup
	handle        *ConsumerHandler
	topic         []string
}

// NewSubscriber 不间断地从队列里捞数据
func NewSubscriber(consumerGroup sarama.ConsumerGroup, schedule func(id string,
	scheduleObject BaseScheduleObject), subscribedTopics []string) (*Subscriber, error) {
	subscriber := &Subscriber{
		Logger:            log.Logger.Get().Named("Subscriber"),
		handle:            NewHandler(schedule),
		consumerGroup:     consumerGroup,
		DefaultPeriodTask: task.NewDefaultPeriodTask("Subscriber", time.Second),
	}

	// 防止消费队列时出现网络短暂的问题，每一分钟刷一次
	runner, err := task.NewPeriodRunner(subscriber)
	if err != nil {
		subscriber.Warn("NewSubscriber-NewPeriodRunnerFailed",
			zap.Error(err),
		)
		return nil, err
	}

	subscriber.Runner = runner
	subscriber.topic = subscribedTopics

	return subscriber, nil
}

func (subscriber *Subscriber) PreExecute() bool {
	subscriber.Info("SubscriberReady")
	return true
}

func (subscriber *Subscriber) Execute() bool {
	err := subscriber.consumerGroup.Consume(subscriber.GetContext(), subscriber.topic, subscriber.handle)
	if err != nil {
		subscriber.Warn("SubscriberConsumeMessageFailed",
			zap.Error(err),
		)
	}
	return true
}

func (subscriber *Subscriber) CleanUp() {
}

type ConsumerHandler struct {
	schedule func(id string, scheduleObject BaseScheduleObject)
	*zap.Logger
}

func NewHandler(schedule func(id string, scheduleObject BaseScheduleObject)) *ConsumerHandler {
	return &ConsumerHandler{
		schedule: schedule,
		Logger:   log.Logger.Get().Named("ConsumerHandler"),
	}
}

// Setup 执行在 获得新 session 后 的第一步, 在 ConsumeClaim() 之前
func (ConsumerHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

// Cleanup 执行在 session 结束前, 当所有 ConsumeClaim goroutines 都退出时
func (ConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim 具体的消费逻辑，会进行监听
func (h ConsumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		id := string(message.Value[:12])
		h.Info("SubscriberPullingByteId",
			zap.String("Id", id),
		)
		// 提交给Scheduler.schedule(id)
		h.schedule(id, nil)
		// 标记消息已被消费 内部会更新 consumer offset
		sess.MarkMessage(message, "")
	}

	return nil
}

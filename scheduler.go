package acclient

import (
	"context"

	"github.com/subiz/header"
	"github.com/subiz/kafka"
	"google.golang.org/protobuf/proto"
)

func BookTask(topic, key, accid string, sec int64, data []byte) {
	kafka.Publish("kafka-1:9092", "scheduler", &header.SchedulerTask{
		AccountId: accid,
		Sec:       sec,
		Payload:   data,
		Topic:     topic,
		Partition: -1,
		Key:       key,
	})
}

func BookTaskP(topic, key, accid string, sec int64, partition int32, data []byte) {
	kafka.Publish("kafka-1:9092", "scheduler", &header.SchedulerTask{
		AccountId: accid,
		Sec:       sec,
		Payload:   data,
		Topic:     topic,
		Partition: partition,
		Key:       key,
	})
}

func OnSchedule(csm, topic string, predicate func(accid string, data []byte)) error {
	_, err := kafka.Consume("kafka-1:9092", csm, topic, func(ctx context.Context, partition int32, offset int64, data []byte, _ string) {
		task := &header.SchedulerTask{}
		proto.Unmarshal(data, task)
		if task.GetAccountId() == "" {
			return
		}
		predicate(task.GetAccountId(), task.GetPayload())
	})
	return err
}

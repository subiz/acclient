package acclient

import (
	"github.com/subiz/header"
	"github.com/subiz/kafka"
	"google.golang.org/protobuf/proto"
)

func BookTask(topic, key, accid string, sec int64, data []byte) {
	kafka.Publish("scheduler", &header.SchedulerTask{
		AccountId: accid,
		Sec:       sec,
		Payload:   data,
		Topic:     topic,
		Partition: -1,
		Key:       key,
	})
}

func BookTaskP(topic, key, accid string, sec int64, partition int32, data []byte) {
	kafka.Publish("scheduler", &header.SchedulerTask{
		AccountId: accid,
		Sec:       sec,
		Payload:   data,
		Topic:     topic,
		Partition: partition,
		Key:       key,
	})
}

func OnSchedule(csm, topic string, predicate func(accid string, data []byte)) error {
	return kafka.Listen(csm, topic, func(partition int32, offset int64, data []byte) {
		task := &header.SchedulerTask{}
		proto.Unmarshal(data, task)
		if task.GetAccountId() == "" {
			kafka.MarkOffset(csm, topic, partition, offset+1)
			return
		}
		predicate(task.GetAccountId(), task.GetPayload())
		kafka.MarkOffset(csm, topic, partition, offset+1)
	})
}

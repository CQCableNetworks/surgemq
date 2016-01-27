package service

import (
	"github.com/surgemq/message"
)

var PendingQueue = make([]*message.PublishMessage, 65000, 65000)
var OfflineTopicQueue = make(map[string][][]byte)
var OfflineTopicQueueProcessor = make(chan *message.PublishMessage)
var Max_message_queue int

func init() {

	go func() {
		for msg := range OfflineTopicQueueProcessor {
			topic := string(msg.Topic())
			new_msg_queue := append(OfflineTopicQueue[topic], msg.Payload())
			length := len(new_msg_queue)
			if length > Max_message_queue {
				OfflineTopicQueue[topic] = new_msg_queue[length-Max_message_queue:]
			} else {
				OfflineTopicQueue[topic] = new_msg_queue
			}
		}
	}()
}

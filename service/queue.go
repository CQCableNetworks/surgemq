package service

import (
	"github.com/surgemq/message"
	"net"
)

var (
	PendingQueue               = make([]*message.PublishMessage, 65000, 65000)
	OfflineTopicQueue          = make(map[string][][]byte)
	OfflineTopicQueueProcessor = make(chan *message.PublishMessage)
	ClientMap                  = make(map[string]*net.Conn)
	ClientMapProcessor         = make(chan ClientHash)
	Max_message_queue          int
)

type ClientHash struct {
	Name string
	Conn *net.Conn
}

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

	go func() {
		for client := range ClientMapProcessor {
			client_id := client.Name
			if ClientMap[client_id] != nil {
				(*ClientMap[client_id]).Close()
			}
			ClientMap[client_id] = client.Conn
		}
	}()
}

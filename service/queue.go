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
	PkgIdProcessor             = make(chan bool)
	PkgIdGenerator             = make(chan uint16)
	Max_message_queue          int
	PkgId                      = uint16(1)
)

type ClientHash struct {
	Name string
	Conn *net.Conn
}

func init() {

	go func() {
		for msg := range OfflineTopicQueueProcessor {
			topic := string(msg.Topic())
			_ = topic
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

	go func() {
		for _ = range PkgIdProcessor {
			PkgIdGenerator <- PkgId
			PkgId = (PkgId % 65535) + 1
		}
	}()
}

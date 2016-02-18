package service

import (
	"github.com/surgemq/message"
	"net"
)

var (
	PendingQueue = make([]*message.PublishMessage, 65536, 65536)

	OfflineTopicQueue          = make(map[string][][]byte)
	OfflineTopicQueueProcessor = make(chan *message.PublishMessage, 2048)

	ClientMap          = make(map[string]*net.Conn)
	ClientMapProcessor = make(chan *ClientHash, 1024)

	PkgIdProcessor = make(chan bool, 1024)
	PkgIdGenerator = make(chan uint16, 1024)
	PkgId          = uint16(1)

	Max_message_queue int
)

type ClientHash struct {
	Name string
	Conn *net.Conn
}

func init() {
	go func() {
		for {
			select {
			case msg := <-OfflineTopicQueueProcessor:
				continue
				topic := string(msg.Topic())
				_ = topic
				new_msg_queue := append(OfflineTopicQueue[topic], msg.Payload())
				length := len(new_msg_queue)
				if length > Max_message_queue {
					OfflineTopicQueue[topic] = new_msg_queue[length-Max_message_queue:]
				} else {
					OfflineTopicQueue[topic] = new_msg_queue
				}

			case client := <-ClientMapProcessor:
				client_id := client.Name
				if ClientMap[client_id] != nil {
					(*ClientMap[client_id]).Close()
				}
				ClientMap[client_id] = client.Conn

			case _ = <-PkgIdProcessor:
				PkgIdGenerator <- PkgId
				PkgId++
			}
		}
	}()
}

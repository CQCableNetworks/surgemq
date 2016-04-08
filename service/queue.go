package service

import (
	"fmt"
	"github.com/nagae-memooff/surgemq/topics"
	"github.com/surgemq/message"
	"net"
	"sync"
	"time"
)

var (
	PendingQueue = make([]*message.PublishMessage, 65536, 65536)
	//   PendingProcessor = make(chan *message.PublishMessage, 65536)

	OfflineTopicMap            = make(map[string]*OfflineTopicQueue)
	OfflineTopicQueueProcessor = make(chan *message.PublishMessage, 2048)
	OfflineTopicCleanProcessor = make(chan string, 2048)
	OfflineTopicPayloadUseGzip bool

	ClientMap          = make(map[string]*net.Conn)
	ClientMapProcessor = make(chan ClientHash, 1024)

	PktId = uint32(1)

	OldMessagesQueue = make(chan *message.PublishMessage, 1024)

	Max_message_queue int
	MessageQueueStore string
)

type ClientHash struct {
	Name string
	Conn *net.Conn
}

// 定义一个离线消息队列的结构体，保存一个二维byte数组和一个位置
type OfflineTopicQueue struct {
	Topic   string
	Q       [][]byte
	Pos     int
	Length  int
	Cleaned bool
	Gziped  bool
	lock    sync.RWMutex
}

func NewOfflineTopicQueue(length int, topic string) (mq *OfflineTopicQueue) {
	var (
		q [][]byte
		t string
	)
	switch MessageQueueStore {
	case "redis":
		t = topic
		q = nil
	case "local":
		t = ""
		q = make([][]byte, length, length)
	}

	mq = &OfflineTopicQueue{
		Topic:   t,
		Q:       q,
		Pos:     0,
		Length:  length,
		Cleaned: true,
		Gziped:  OfflineTopicPayloadUseGzip,
	}

	return mq
}

// 向队列中添加消息
//NOTE 因为目前是在channel中操作，所以无需加锁。如果需要并发访问，则需要加锁了。
func (this *OfflineTopicQueue) Add(msg_bytes []byte) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.Q == nil && MessageQueueStore != "redis" {
		this.Q = make([][]byte, this.Length, this.Length)
	}

	// 判断是否gzip，如果是，则压缩后再存入
	if this.Gziped {
		mb, err := Gzip(msg_bytes)
		if err != nil {
			Log.Errorc(func() string { return fmt.Sprintf("gzip failed. msg: %s, err: %s", msg_bytes, err) })
			return
		}

		switch MessageQueueStore {
		case "local":
			this.Q[this.Pos] = mb
		case "redis":
			topics.RedisDo("set", this.RedisKey(this.Pos), mb)
		}
	} else {
		switch MessageQueueStore {
		case "local":
			this.Q[this.Pos] = msg_bytes
		case "redis":
			topics.RedisDo("set", this.RedisKey(this.Pos), msg_bytes)
		}
	}

	this.Pos++

	if this.Pos >= this.Length {
		this.Pos = 0
	}

	if this.Cleaned {
		this.Cleaned = false
	}
}

// 清除队列中已有消息
func (this *OfflineTopicQueue) Clean() {
	this.lock.Lock()
	defer this.lock.Unlock()
	switch MessageQueueStore {
	case "local":
		this.Q = nil
	case "redis":
		keys := make([]interface{}, this.Length, this.Length)
		for i := 0; i < this.Length; i++ {
			keys[i] = this.RedisKey(i)
		}

		topics.RedisDo("del", keys...)
	}

	this.Pos = 0
	this.Cleaned = true
	this.Gziped = OfflineTopicPayloadUseGzip
}

func (this *OfflineTopicQueue) GetAll() (msg_bytes [][]byte) {
	if this.Cleaned {
		return nil
	} else {
		this.lock.RLock()
		defer this.lock.RUnlock()

		msg_bytes = make([][]byte, this.Length, this.Length)
		switch MessageQueueStore {
		case "local":
			msg_bytes = this.Q[this.Pos:this.Length]
			msg_bytes = append(msg_bytes, this.Q[0:this.Pos]...)
		case "redis":
			keys := make([]interface{}, this.Length, this.Length)
			for i := this.Pos; i < this.Length; i++ {
				keys = append(keys, this.RedisKey(i))
			}
			for i := 0; i < this.Pos; i++ {
				keys = append(keys, this.RedisKey(i))
			}

			var err error
			msg_bytes, err = topics.RedisDoGetMultiByteSlice("mget", keys...)
			if err != nil {
				Log.Errorc(func() string { return err.Error() })
			}
		}

		// 判断是否gzip存储，如果是，则解压后再取出
		if this.Gziped {
			for i, bytes := range msg_bytes {
				if bytes != nil {
					mb, err := Gunzip(bytes)
					if err != nil {
						Log.Errorc(func() string { return err.Error() })
					}

					msg_bytes[i] = mb
				}
			}
		}
		return msg_bytes
	}
}

func (this *OfflineTopicQueue) RedisKey(pos int) (key string) {
	return fmt.Sprintf("/t/%s:%d", this.Topic, pos)
}

//如果是redis内的，则不支持转换
func (this *OfflineTopicQueue) ConvertToGzip() (err error) {
	if this.Cleaned {
		return nil
	} else if MessageQueueStore == "local" && !this.Gziped {
		this.Gziped = true
		for i, bytes := range this.Q {
			if bytes != nil {
				mb, err := Gzip(bytes)
				if err != nil {
					Log.Errorc(func() string { return err.Error() })
				}

				this.Q[i] = mb
			}
		}
	}
	return nil
}

//如果是redis内的，则不支持转换
func (this *OfflineTopicQueue) ConvertToUnzip() (err error) {
	if this.Cleaned {
		return nil
	} else if MessageQueueStore == "local" && this.Gziped {
		this.Gziped = false
		for i, bytes := range this.Q {
			if bytes != nil {
				mb, err := Gunzip(bytes)
				if err != nil {
					Log.Errorc(func() string { return err.Error() })
				}

				this.Q[i] = mb
			}
		}
	}
	return nil
}

func init() {
	/*
		for i := 0; i < 1024; i++ {
			sub_p := make([]interface{}, 1, 1)
			select {
			case SubscribersSliceQueue <- sub_p:
			default:
				sub_p = nil
				return
			}
		}
	*/

	/*
		for i := 0; i < 15000; i++ {
			tmp_msg := message.NewPublishMessage()
			tmp_msg.SetQoS(message.QosAtLeastOnce)

			NewMessagesQueue <- tmp_msg
		}
	*/
	go func() {
		for {
			select {
			case msg := <-OldMessagesQueue:
				MessagePool.Put(msg)
				//
			default:
				time.Sleep(10 * time.Second)

			}

		}
	}()

	go func() {
		for {
			select {
			case topic := <-OfflineTopicCleanProcessor:
				Log.Debugc(func() string {
					return fmt.Sprintf("clean offlie topic queue: %s", topic)
				})

				q := OfflineTopicMap[topic]
				if q != nil {
					go q.Clean()
				}
			case msg := <-OfflineTopicQueueProcessor:
				//         _ = msg
				topic := string(msg.Topic())
				q := OfflineTopicMap[topic]
				if q == nil {
					q = NewOfflineTopicQueue(Max_message_queue, topic)

					OfflieTopicRWmux.Lock()
					OfflineTopicMap[topic] = q
					OfflieTopicRWmux.Unlock()
				}

				q.Add(msg.Payload())

				Log.Debugc(func() string {
					return fmt.Sprintf("add offline message to the topic: %s", topic)
				})

			case client := <-ClientMapProcessor:
				client_id := client.Name
				client_conn := client.Conn

				if ClientMap[client_id] != nil {
					old_conn := *ClientMap[client_id]
					old_conn.Close()

					Log.Debugc(func() string {
						return fmt.Sprintf("client connected with same client_id: %s. close old connection.", client_id)
					})
				}
				ClientMap[client_id] = client_conn

			}
		}
	}()
}

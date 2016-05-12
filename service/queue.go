package service

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/nagae-memooff/surgemq/topics"
	"github.com/surgemq/message"
	"net"
	"sync"
	"time"
)

var (
	PendingQueue = make([]*PendingStatus, 65536, 65536)
	//   PendingProcessor = make(chan *message.PublishMessage, 65536)

	OfflineTopicMap            = make(map[string]*OfflineTopicQueue)
	OfflineTopicQueueProcessor = make(chan *message.PublishMessage, 2048)
	OfflineTopicCleanProcessor = make(chan string, 2048)
	OfflineTopicPayloadUseGzip bool

	ClientMap          = make(map[string]*net.Conn)
	ClientMapProcessor = make(chan ClientHash, 1024)

	PktId = uint32(1)

	OldMessagesQueue = make(chan *message.PublishMessage, 8192)

	Max_message_queue int
	MessageQueueStore string
	temp_bytes        *sync.Pool
)

type PendingStatus struct {
	Done  chan (bool)
	Topic string
}

func NewPendingStatus(topic string) *PendingStatus {
	return &PendingStatus{
		Done:  make(chan bool),
		Topic: topic,
	}
}

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
			_, err := topics.RedisDo("set", this.RedisKey(this.Pos), mb)
			if err != nil {
				Log.Errorc(func() string {
					return fmt.Sprintf("failed to save offline msg to redis: %s", err.Error())
				})
			}
		}
	} else {
		switch MessageQueueStore {
		case "local":
			this.Q[this.Pos] = msg_bytes
		case "redis":
			_, err := topics.RedisDo("set", this.RedisKey(this.Pos), msg_bytes)
			if err != nil {
				Log.Errorc(func() string {
					return fmt.Sprintf("failed to save offline msg to redis: %s", err.Error())
				})
			}
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
		if this.Length == Max_message_queue {
			for i, _ := range this.Q {
				this.Q[i] = nil
			}
		} else {
			this.Q = nil
		}
	case "redis":
		//     keys := make([]string, this.Length, this.Length)
		keys := redis.Args{}
		for i := 0; i < this.Length; i++ {
			keys.Add(this.RedisKey(i))
		}

		_, err := topics.RedisDoDel(keys)
		if err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("failed to clean offline msg to redis: %s", err.Error())
			})
		}
	}

	this.Pos = 0
	this.Cleaned = true
	this.Length = Max_message_queue
	this.Gziped = OfflineTopicPayloadUseGzip
}

func (this *OfflineTopicQueue) GetAll() (msg_bytes [][]byte) {
	if this.Cleaned {
		return nil
	} else {
		this.lock.RLock()
		defer this.lock.RUnlock()

		//     msg_bytes = make([][]byte, this.Length, this.Length)
		//     msg_bytes = temp_bytes.Get().([][]byte)
		var msg_bytes [][]byte

		switch MessageQueueStore {
		case "local":
			msg_bytes = this.Q[this.Pos:this.Length]
			msg_bytes = append(msg_bytes, this.Q[0:this.Pos]...)
		case "redis":
			var keys []interface{}
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

	temp_bytes = &sync.Pool{
		New: func() interface{} {
			return make([][]byte, Max_message_queue, Max_message_queue)
		},
	}

	go func() {
		for {
			select {
			case msg := <-OldMessagesQueue:
				MessagePool.Put(msg)
				//
			default:
				time.Sleep(5 * time.Second)

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

				OfflineTopicRWmux.RLock()
				q := OfflineTopicMap[topic]
				OfflineTopicRWmux.RUnlock()
				if q != nil {
					go q.Clean()
				}
			case msg := <-OfflineTopicQueueProcessor:
				//         _ = msg
				//         topic := string(msg.Topic())
				go func(topic string, payload []byte) {
					//         func(topic string, payload []byte) {
					OfflineTopicRWmux.RLock()
					q := OfflineTopicMap[topic]
					OfflineTopicRWmux.RUnlock()
					if q == nil {
						q = NewOfflineTopicQueue(Max_message_queue, topic)

						OfflineTopicRWmux.Lock()
						OfflineTopicMap[topic] = q
						OfflineTopicRWmux.Unlock()
					}

					Log.Debugc(func() string {
						return fmt.Sprintf("add offline message to the topic: %s", topic)
						// return fmt.Sprintf("add offline message: topic: %s, payload: %s", msg.Topic(), msg.Payload())
					})

					q.Add(payload)
					_return_tmp_msg(msg)
				}(string(msg.Topic()), msg.Payload())

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

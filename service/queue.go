package service

import (
	"fmt"
	"github.com/nagae-memooff/config"
	"github.com/nagae-memooff/surgemq/topics"
	"github.com/surgemq/message"
	"github.com/syndtr/goleveldb/leveldb"
	"net"
	"sync"
	"time"
)

var (
	PendingQueue = make([]*PendingStatus, 65536, 65536)
	//   PendingProcessor = make(chan *message.PublishMessage, 65536)

	OfflineTopicMap            = make(map[string]*OfflineTopicQueue)
	OfflineTopicQueueProcessor = make(chan *message.PublishMessage, 8192)
	OfflineTopicCleanProcessor = make(chan string, 128)

	ClientMap          = make(map[string]*net.Conn)
	ClientMapProcessor = make(chan ClientHash, 1024)

	PktId = uint32(1)

	OldMessagesQueue = make(chan *message.PublishMessage, 8192)

	Max_message_queue int
	MessageQueueStore string
	TempBytes         *sync.Pool

	LevelDB *leveldb.DB
)

type PendingStatus struct {
	Done  chan (bool)
	Topic string
	Msg   *message.PublishMessage
}

func NewPendingStatus(topic string, msg *message.PublishMessage) *PendingStatus {
	return &PendingStatus{
		Done:  make(chan bool),
		Topic: topic,
		Msg:   msg,
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
	Cleaned bool
	lock    sync.RWMutex
}

func NewOfflineTopicQueue(topic string) (mq *OfflineTopicQueue) {
	var (
		q [][]byte
		t string
	)
	switch MessageQueueStore {
	case "local":
		t = ""
		q = make([][]byte, Max_message_queue, Max_message_queue)
	case "redis":
		t = topic
		q = nil
	case "leveldb":
		t = topic
		q = nil
	}

	mq = &OfflineTopicQueue{
		Topic:   t,
		Q:       q,
		Pos:     0,
		Cleaned: true,
	}

	return mq
}

// 向队列中添加消息
func (this *OfflineTopicQueue) Add(msg_bytes []byte) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.Q == nil && MessageQueueStore == "local" {
		this.Q = make([][]byte, Max_message_queue, Max_message_queue)
	}

	switch MessageQueueStore {
	case "local":
		this.Q[this.Pos] = msg_bytes
	case "redis":
		_, err := topics.RedisDo("set", this.DBKey(this.Pos), msg_bytes)
		if err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("failed to save offline msg to redis: %s", err.Error())
			})
		}
	case "leveldb":
		err := LevelDB.Put([]byte(this.DBKey(this.Pos)), msg_bytes, nil)
		if err != nil {
			Log.Error("leveldb: %s", err.Error())
		}
	}

	this.Pos++

	if this.Pos >= Max_message_queue {
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
		for i, _ := range this.Q {
			this.Q[i] = nil
		}
	case "redis":
		//     keys := make([]interface{}, this.Length, this.Length)
		keys := TempBytes.Get().([]interface{})
		for i := 0; i < Max_message_queue; i++ {
			keys[i] = this.DBKey(i)
		}

		_, err := topics.RedisDoDel(keys...)
		if err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("failed to clean offline msg to redis: %s", err.Error())
			})
		}
	case "leveldb":
		keys := TempBytes.Get().([][]byte)
		//     keys := make([][]byte, this.Length, this.Length)
		for i := 0; i < Max_message_queue; i++ {
			keys[i] = []byte(this.DBKey(i))
		}

		var err error
		for _, key := range keys {
			err = LevelDB.Delete(key, nil)
			if err != nil {
				Log.Errorc(func() string {
					return fmt.Sprintf("failed to clean offline msg from leveldb: %s", err.Error())
				})
			}
		}
	}

	this.Pos = 0
	this.Cleaned = true
}

func (this *OfflineTopicQueue) GetAll() (msg_bytes [][]byte) {
	if this.Cleaned {
		return nil
	} else {
		this.lock.RLock()
		defer this.lock.RUnlock()

		msg_bytes = TempBytes.Get().([][]byte)
		//     var msg_bytes [][]byte

		switch MessageQueueStore {
		case "local":
			msg_bytes = this.Q[this.Pos:Max_message_queue]
			msg_bytes = append(msg_bytes, this.Q[0:this.Pos]...)
		case "redis":
			//       keys := make([]interface{}, 0, Max_message_queue)
			keys := TempBytes.Get().([]interface{})
			for i := this.Pos; i < Max_message_queue; i++ {
				keys = append(keys, this.DBKey(i))
			}
			for i := 0; i < this.Pos; i++ {
				keys = append(keys, this.DBKey(i))
			}

			var err error
			msg_bytes, err = topics.RedisDoGetMultiByteSlice("mget", keys...)
			if err != nil {
				Log.Errorc(func() string { return err.Error() })
			}
		case "leveldb":
			//       var keys [][]byte
			keys := TempBytes.Get().([][]byte)
			for i := this.Pos; i < Max_message_queue; i++ {
				keys = append(keys, []byte(this.DBKey(i)))
			}
			for i := 0; i < this.Pos; i++ {
				keys = append(keys, []byte(this.DBKey(i)))
			}

			for _, key := range keys {
				data, err := LevelDB.Get(key, nil)
				if err != nil {
					continue
				}
				// Log.Debug("key is %s, value is %s", key, data)

				msg_bytes = append(msg_bytes, data)
			}

		}

		return msg_bytes
	}
}

func (this *OfflineTopicQueue) DBKey(pos int) (key string) {
	return fmt.Sprintf("/t/%s:%d", this.Topic, pos)
}

func init() {

	TempBytes = &sync.Pool{
		New: func() interface{} {
			return make([][]byte, 0, Max_message_queue)
		},
	}

	go func() {
		sleep_time := time.Duration(config.GetInt("publish_timeout_second")+10) * time.Second
		for {
			select {
			case msg := <-OldMessagesQueue:
				MessagePool.Put(msg)
				//
			default:
				time.Sleep(sleep_time)

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
				go func(topic_bytes, payload []byte) {
					//         func(topic string, payload []byte) {
					topic := string(topic_bytes)
					OfflineTopicRWmux.RLock()
					q := OfflineTopicMap[topic]
					OfflineTopicRWmux.RUnlock()
					if q == nil {
						q = NewOfflineTopicQueue(topic)

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
				}(msg.Topic(), msg.Payload())

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

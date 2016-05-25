package service

import (
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
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
	OfflineTopicPayloadUseGzip bool

	ClientMap          = make(map[string]*net.Conn)
	ClientMapProcessor = make(chan ClientHash, 1024)

	PktId = uint32(1)

	OldMessagesQueue = make(chan *message.PublishMessage, 8192)

	Max_message_queue int
	MessageQueueStore string
	temp_bytes        *sync.Pool

	BoltDB  *bolt.DB
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
	Topic      string
	Q          [][]byte
	Pos        int
	Length     int
	Cleaned    bool
	Gziped     bool
	TopicBytes []byte
	lock       sync.RWMutex
}

func NewOfflineTopicQueue(length int, topic string) (mq *OfflineTopicQueue) {
	var (
		q  [][]byte
		t  string
		tb []byte
	)
	switch MessageQueueStore {
	case "local":
		t = ""
		q = make([][]byte, length, length)
		tb = nil
	case "redis":
		t = topic
		q = nil
		tb = nil
	case "bolt":
		t = topic
		q = nil
		tb = []byte(topic)
	case "leveldb":
		t = topic
		q = nil
		tb = nil
	}

	mq = &OfflineTopicQueue{
		Topic:      t,
		Q:          q,
		Pos:        0,
		Length:     length,
		Cleaned:    true,
		TopicBytes: tb,
		Gziped:     OfflineTopicPayloadUseGzip,
	}

	return mq
}

// 向队列中添加消息
func (this *OfflineTopicQueue) Add(msg_bytes []byte) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.Q == nil && MessageQueueStore == "local" {
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
		case "bolt":
			BoltDB.Update(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists(this.TopicBytes)
				if err != nil {
					return err
				}
				return b.Put(this.PosToB(), mb)
			})
		case "leveldb":
			err := LevelDB.Put(this.LevelDBKey(this.Pos), mb, nil)
			if err != nil {
				Log.Error("leveldb: %s", err.Error())
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
		case "bolt":
			BoltDB.Update(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists(this.TopicBytes)
				if err != nil {
					return err
				}
				return b.Put(this.PosToB(), msg_bytes)
			})
		case "leveldb":
			err := LevelDB.Put(this.LevelDBKey(this.Pos), msg_bytes, nil)
			if err != nil {
				Log.Error("leveldb: %s", err.Error())
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
		keys := make([]interface{}, this.Length, this.Length)
		for i := 0; i < this.Length; i++ {
			keys[i] = this.RedisKey(i)
		}

		_, err := topics.RedisDoDel(keys...)
		if err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("failed to clean offline msg to redis: %s", err.Error())
			})
		}
	case "bolt":
		BoltDB.Update(func(tx *bolt.Tx) error {
			err := tx.DeleteBucket(this.TopicBytes)

			return err
		})
	case "leveldb":
		keys := make([][]byte, this.Length, this.Length)
		for i := 0; i < this.Length; i++ {
			keys[i] = this.LevelDBKey(i)
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
		case "bolt":
			BoltDB.View(func(tx *bolt.Tx) error {
				b := tx.Bucket(this.TopicBytes)

				var keys [][]byte
				for i := this.Pos; i < this.Length; i++ {
					keys = append(keys, PosToB(i))
				}
				for i := 0; i < this.Pos; i++ {
					keys = append(keys, PosToB(i))
				}

				for _, key := range keys {
					msg_bytes = append(msg_bytes, b.Get(key))
				}

				return nil
			})

		case "leveldb":
			var keys [][]byte
			for i := this.Pos; i < this.Length; i++ {
				keys = append(keys, this.LevelDBKey(i))
			}
			for i := 0; i < this.Pos; i++ {
				keys = append(keys, this.LevelDBKey(i))
			}

			for _, key := range keys {
				data, err := LevelDB.Get(key, nil)
				if err != nil {
					Log.Errorc(func() string {
						return fmt.Sprintf("failed to get offline msg from leveldb: %s", err.Error())
					})
					continue
				}
				// Log.Debug("key is %s, value is %s", key, data)

				msg_bytes = append(msg_bytes, data)
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

func (this *OfflineTopicQueue) LevelDBKey(pos int) (key []byte) {
	return []byte(fmt.Sprintf("/t/%s:%d", this.Topic, pos))
}

func (this *OfflineTopicQueue) PosToB() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(this.Pos))
	return b
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

func PosToB(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

func BToPOS(b []byte) int {
	return int(binary.BigEndian.Uint64(b))
}

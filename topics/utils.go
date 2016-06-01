package topics

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/nagae-memooff/config"
	"github.com/surgemq/message"
	"strings"
	"sync"
	"time"
)

var (
	MXMaxQosAllowed     = message.QosAtLeastOnce
	RedisPool           *redis.Pool
	Channelcache        map[string]string
	ChannelReversecache map[string]string
	Cmux                sync.RWMutex
)

func init() {
	Channelcache = make(map[string]string)
	ChannelReversecache = make(map[string]string)
}

func NewRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:   config.GetInt("redis_max_idle"),
		MaxActive: config.GetInt("redis_concurrent"), // max number of connections
		Dial: func() (redis.Conn, error) {
			var (
				c   redis.Conn
				err error
			)
			redis_host := fmt.Sprintf("%s:%s", config.GetMulti("redis_host", "redis_port")...)

			redis_passwd := config.Get("redis_passwd")
			if redis_passwd != "" {
				pwd := redis.DialPassword(redis_passwd)
				c, err = redis.Dial("tcp", redis_host, pwd)
			} else {
				c, err = redis.Dial("tcp", redis_host)
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func RedisDo(cmd string, args ...interface{}) (data string, err error) {
	c := RedisPool.Get()
	defer c.Close()

	data, err = redis.String(c.Do(cmd, args...))
	if err != nil {
		fmt.Println(err)
	}

	return
}

func RedisDoDel(args ...interface{}) (data int, err error) {
	c := RedisPool.Get()
	defer c.Close()

	data, err = redis.Int(c.Do("del", args...))
	if err != nil {
		fmt.Println(err)
	}

	return
}

func GetClientIDandChannels() (client_ids []string, channels []interface{}, err error) {
	c := RedisPool.Get()
	defer c.Close()

	data, err := redis.Strings(c.Do("keys", "channel:*"))

	for _, channel := range data {
		channels = append(channels, channel)

		client_id := strings.Split(channel, ":")[1]
		client_ids = append(client_ids, client_id)
	}
	return
}

func RedisDoGetMulti(cmd string, args ...interface{}) (data []string, err error) {
	c := RedisPool.Get()
	defer c.Close()

	//   data_origin, err := c.Do("keys", "channel:*")
	data, err = redis.Strings(c.Do(cmd, args...))
	if err != nil {
		fmt.Println(err)
	}

	return
}

func RedisDoGetMultiByteSlice(cmd string, args ...interface{}) (data [][]byte, err error) {
	c := RedisPool.Get()
	defer c.Close()

	//   data_origin, err := c.Do("keys", "channel:*")
	data, err = redis.ByteSlices(c.Do(cmd, args...))
	if err != nil {
		fmt.Println(err)
	}

	return
}

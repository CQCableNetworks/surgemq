package topics

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/nagae-memooff/config"
)

func newRedisPool() *redis.Pool {
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

			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}
}

func RedisDo(cmd string, args ...interface{}) (data string, err error) {
	c := RedisPool.Get()
	defer func() {
		c.Close()
	}()

	data_origin, err := c.Do(cmd, args...)
	if err != nil {
		fmt.Println(err)
		return
	}

	data_byte, ok := data_origin.([]byte)
	if !ok {
		return
	}

	return (string)(data_byte), nil
}

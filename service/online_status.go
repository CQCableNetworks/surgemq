package service

import (
	"github.com/nagae-memooff/surgemq/topics"
	"io"
	"sync"
	"time"
)

var (
	list map[string]*Status
	lock sync.RWMutex
)

type Status struct {
	online   bool
	lastTime time.Time
	conn     *io.Closer
}

func (this *Status) IsOnline() bool {
	if this == nil {
		return false
	} else {
		return this.online
	}
}

func (this *Status) OnlineStatus() string {
	if this.IsOnline() {
		return "online"
	} else {
		return "offline"
	}
}

func (this *Status) LastTime() (t time.Time) {
	if this == nil {
		return t
	} else {
		return this.lastTime
	}
}

func (this *Status) Conn() *io.Closer {
	if this == nil {
		return nil
	} else {
		return this.conn
	}
}

func GetOnlineStatus(key string) (online string, lasttime time.Time, conn *io.Closer) {
	lock.RLock()
	defer lock.RUnlock()

	status := list[key]

	online = status.OnlineStatus()
	lasttime = status.LastTime()
	conn = status.Conn()
	return
}

func SetOnlineStatus(key string, online bool, lasttime time.Time, conn *io.Closer) {
	lock.Lock()
	defer lock.Unlock()
	status := list[key]

	if status == nil {
		status = &Status{
			online,
			lasttime,
			conn,
		}
		list[key] = status
	} else {
		status.online = online
		status.lastTime = lasttime
		status.conn = conn
	}

}

func init() {
	list = make(map[string]*Status)
}

func IsOnline(topic string) (online bool) {
	if topic == OnlineStatusChannel {
		return true
	}

	lock.RLock()
	defer lock.RUnlock()

	topics.Cmux.RLock()
	client_id := topics.ChannelReversecache[topic]
	topics.Cmux.RUnlock()

	return list[client_id].IsOnline()
}

package service

import (
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

func GetOnlineStatus(key string) (online string, lasttime time.Time) {
	lock.RLock()
	defer lock.RUnlock()

	status := list[key]

	online = status.OnlineStatus()
	lasttime = status.LastTime()
	return
}

func SetOnlineStatus(key string, online bool, lasttime time.Time) {
	lock.Lock()
	status := list[key]

	if status == nil {
		status = &Status{
			online,
			lasttime,
		}
		list[key] = status
	} else {
		status.online = online
		status.lastTime = lasttime
	}

	lock.Unlock()
}

func init() {
	list = make(map[string]*Status)
}

package service

import (
	//   "github.com/nagae-memooff/surgemq/auth"
	//   "github.com/nagae-memooff/surgemq/sessions"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/nagae-memooff/surgemq/topics"
	"github.com/pquerna/ffjson/ffjson"
	apns "github.com/sideshow/apns2"
	"github.com/surgemq/message"
	"strings"
	"time"
)

var (
	Cert      tls.Certificate
	APNsTopic string
)

var mxOnGroupPublish = func(msg *message.PublishMessage, this *service) (err error) {
	var (
		broadcast_msg BroadCastMessage
		payload       []byte
	)

	err = ffjson.Unmarshal(msg.Payload(), &broadcast_msg)
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("can't parse message json: %s", msg.Payload())
		})
		return
	}

	payload, err = base64.StdEncoding.DecodeString(broadcast_msg.Payload)
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("can't decode payload: %s", broadcast_msg.Payload)
		})
		return
	}

	n := 0
	for _, client_id := range broadcast_msg.Clients {
		topic := topics.GetUserTopic(client_id)
		if topic == "" {
			continue
		}
		n++

		go this.publishToTopic(topic, payload)
	}

	Log.Infoc(func() string {
		return fmt.Sprintf("(%s) process group message to %d/%d clients. payload size: %d", this.cid(), n, len(broadcast_msg.Clients), len(payload))
	})

	return
}

var mxProcessAck = func(pkt_id uint16, this *service) {
	pending_status := PendingQueue[pkt_id]
	if pending_status == nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) receive ack, but this pkt_id %d in queue is nil! ", this.cid(), pkt_id)
		})

		return
	}

	topic := topics.GetUserTopic(this.sess.ID())

	if pending_status.Topic == OnlineStatusChannel {
		return
	}

	if pending_status.Topic != topic {
		// ack包的topic,与登记的不一致
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) receive ack, but the topic %s and %s is mismatch. ", this.cid(), pending_status.Topic, topic)
		})
		return
	}

	select {
	case pending_status.Done <- true:
		Log.Debugc(func() string {
			return fmt.Sprintf("(%s) receive ack, remove msg from pending queue: %d", this.cid(), pkt_id)
		})
	case <-time.After(time.Second * MsgPendingTime):
		//说明有问题，只有两种情况： 堵死或者nil。
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) receive ack %d but already timeout. payload len: %d.", this.cid(), pkt_id, len(pending_status.Msg.Payload()))
		})
	}
}

var mxIsOnline = func(topic string) (online bool) {
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

var mtOnGroupPublish = func(msg *message.PublishMessage, this *service) (err error) {
	var (
		broadcast_msg MtBroadCastMessage
		payload       []byte
	)

	Log.Infoc(func() string {
		return "receive group msgs.\n"
	})

	err = ffjson.Unmarshal(msg.Payload(), &broadcast_msg)
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("can't parse message json: %s", msg.Payload())
		})
		return
	}

	payload, err = base64.StdEncoding.DecodeString(broadcast_msg.Payload)
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("can't decode payload: %s", broadcast_msg.Payload)
		})
		return
	}

	for _, topic := range broadcast_msg.Clients {
		go this.publishToTopic(topic, payload)
	}

	Log.Infoc(func() string {
		return fmt.Sprintf("(%s) receive group message. clients: %d, payload size: %d", this.cid(), len(broadcast_msg.Clients), len(payload))
	})

	return
}

var mtProcessAck = func(pkt_id uint16, this *service) {
	pending_status := PendingQueue[pkt_id]
	if pending_status == nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) receive ack, but this pkt_id %d in queue is nil! ", this.cid(), pkt_id)
		})

		return
	}

	topic := topics.MTGetUserTopic(this.sess.ID())
	if pending_status.Topic == OnlineStatusChannel {
		return
	}

	if pending_status.Topic != topic {
		// ack包的topic,与登记的不一致
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) receive ack, but the topic %s and %s is mismatch. ", this.cid(), pending_status.Topic, topic)
		})
		return
	}

	select {
	case pending_status.Done <- true:
		Log.Debugc(func() string {
			return fmt.Sprintf("(%s) receive ack, remove msg from pending queue: %d", this.cid(), pkt_id)
		})
	case <-time.After(time.Second * MsgPendingTime):
		//说明有问题，只有两种情况： 堵死或者nil。
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) receive ack, but this value of this pkt_id %d in queue is %v. ", this.cid(), pkt_id, pending_status)
		})
	}
}

var mtIsOnline = func(topic string) (online bool) {
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

// 处理苹果推送
func onAPNsPush(msg *message.PublishMessage, this *service) (err error) {
	Log.Infoc(func() string {
		return fmt.Sprintf("(%s) receive apn message.", this.cid())
	})

	args := strings.SplitN(string(msg.Payload()), ":", 2)
	if len(args) != 2 {
		err = errors.New(fmt.Sprintf("(%s) parse apn message failed! payload is %s ", this.cid(), msg.Payload()))

		Log.Errorc(func() string {
			return err.Error()
		})

		return
	}

	token := args[0]
	msg_json := args[1]

	notification := &apns.Notification{}
	notification.DeviceToken = token
	notification.Topic = APNsTopic
	notification.Payload = []byte(msg_json) // See Payload section below

	client := apns.NewClient(Cert).Production()
	res, err := client.Push(notification)

	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) push apn message failed. err: %s, res: %s", this.cid(), err, res)
		})
	}

	return
}

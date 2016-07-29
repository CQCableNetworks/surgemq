// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"encoding/base64"
	"github.com/pquerna/ffjson/ffjson"
	//   "encoding/json"
	"errors"
	"fmt"
	//   "io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//   "runtime/debug"
	"github.com/nagae-memooff/surgemq/sessions"
	"github.com/nagae-memooff/surgemq/topics"
	"github.com/surgemq/message"
)

var (
	errDisconnect       = errors.New("Disconnect")
	MsgPendingTime      time.Duration
	OfflineTopicRWmux   sync.RWMutex
	BroadCastChannel    string
	SendChannel         string
	ApnPushChannel      string
	p                   *sync.Pool
	MessagePool         *sync.Pool
	OnGroupPublish      func(msg *message.PublishMessage, this *service) (err error)
	processAck          func(pkt_id uint16, this *service)
	OnlineStatusChannel = "/fdf406fadef0ba24f3bfe8bc00b7bb350901417f"
)

func init() {
	p = &sync.Pool{
		New: func() interface{} {
			return make([]interface{}, 1, 1)

		},
	}

	MessagePool = &sync.Pool{
		New: func() interface{} {
			tmp_msg := message.NewPublishMessage()
			tmp_msg.SetQoS(message.QosAtLeastOnce)
			return tmp_msg

		},
	}

}

// processor() reads messages from the incoming buffer and processes them
func (this *service) processor() {
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("(%s) Recovering from panic: %v", this.cid(), r)
			})
		}

		this.wgStopped.Done()
		this.stop()

		Log.Debugc(func() string {
			return fmt.Sprintf("(%s) Stopping processor", this.cid())
		})
	}()

	this.wgStarted.Done()

	for {
		// 1. Find out what message is next and the size of the message
		p, ok := this.in.ReadBuffer()
		if !ok {
			Log.Debugc(func() string {
				return fmt.Sprintf("(%s) suddenly disconnect.", this.cid())
			})
			return
		}
		mtype := message.MessageType((*p)[0] >> 4)

		_p := this.server.getRealBytes(p)

		msg, err := mtype.New()
		n, err := msg.Decode(_p)
		//清理指针p
		//p = nil
		_p = nil
		this.server.DestoryBytes(*p)
		if err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("(%s) Error peeking next message: %v", this.cid(), err)
			})
			return
		}
		//     this.rmu.Unlock()

		//Log.Debugc(func() string{ return fmt.Sprintf("(%s) Received: %s", this.cid(), msg)})

		this.inStat.increment(int64(n))

		// 5. Process the read message
		err = this.processIncoming(msg)
		if err != nil {
			if err != errDisconnect {
				Log.Errorc(func() string {
					return fmt.Sprintf("(%s) Error processing %s: %v", this.cid(), msg.Name(), err)
				})
			} else {
				return
			}
		}

		// 7. Check to see if done is closed, if so, exit
		if this.isDone() {
			return
		}

	}
}

func (this *service) processIncoming(msg message.Message) error {
	var err error = nil
	//   Log.Errorc(func() string{ return fmt.Sprintf("this.subs is: %v,  count is %d, msg_type is %T", this.subs, len(this.subs), msg)})

	switch msg := (msg).(type) {
	case *message.PublishMessage:
		// For PUBLISH message, we should figure out what QoS it is and process accordingly
		// If QoS == 0, we should just take the next step, no ack required
		// If QoS == 1, we should send back PUBACK, then take the next step
		// If QoS == 2, we need to put it in the ack queue, send back PUBREC
		//     (*msg).SetPacketId(getRandPkgId())
		//     Log.Errorc(func() string{ return fmt.Sprintf("\n%T:%d==========\nmsg is %v\n=====================", *msg, msg.PacketId(), *msg)})
		err = this.processPublish(msg)

	case *message.PubackMessage:
		//     Log.Errorc(func() string{ return fmt.Sprintf("this.subs is: %v,  count is %d, msg_type is %T", this.subs, len(this.subs), msg)})
		// For PUBACK message, it means QoS 1, we should send to ack queue
		//     Log.Errorc(func() string{ return fmt.Sprintf("\n%T:%d==========\nmsg is %v\n=====================", *msg, msg.PacketId(), *msg)})
		go processAck(msg.PacketId(), this)
		this.sess.Pub1ack.Ack(msg)
		this.processAcked(this.sess.Pub1ack)

	case *message.PubrecMessage:
		// For PUBREC message, it means QoS 2, we should send to ack queue, and send back PUBREL
		if err = this.sess.Pub2out.Ack(msg); err != nil {
			break
		}

		resp := message.NewPubrelMessage()
		resp.SetPacketId(msg.PacketId())
		_, err = this.writeMessage(resp)

	case *message.PubrelMessage:
		// For PUBREL message, it means QoS 2, we should send to ack queue, and send back PUBCOMP
		if err = this.sess.Pub2in.Ack(msg); err != nil {
			break
		}

		this.processAcked(this.sess.Pub2in)

		resp := message.NewPubcompMessage()
		resp.SetPacketId(msg.PacketId())
		_, err = this.writeMessage(resp)

	case *message.PubcompMessage:
		// For PUBCOMP message, it means QoS 2, we should send to ack queue
		if err = this.sess.Pub2out.Ack(msg); err != nil {
			break
		}

		this.processAcked(this.sess.Pub2out)

	case *message.SubscribeMessage:
		// For SUBSCRIBE message, we should add subscriber, then send back SUBACK
		return this.processSubscribe(msg)

	case *message.SubackMessage:
		// For SUBACK message, we should send to ack queue
		this.sess.Suback.Ack(msg)
		this.processAcked(this.sess.Suback)

	case *message.UnsubscribeMessage:
		// For UNSUBSCRIBE message, we should remove subscriber, then send back UNSUBACK
		return this.processUnsubscribe(msg)

	case *message.UnsubackMessage:
		// For UNSUBACK message, we should send to ack queue
		this.sess.Unsuback.Ack(msg)
		this.processAcked(this.sess.Unsuback)

	case *message.PingreqMessage:
		// For PINGREQ message, we should send back PINGRESP
		//     Log.Debugc(func() string { return fmt.Sprintf("(%s) receive pingreq.", this.cid()) })
		resp := message.NewPingrespMessage()
		_, err = this.writeMessage(resp)

	case *message.PingrespMessage:
		//     Log.Debugc(func() string { return fmt.Sprintf("(%s) receive pingresp.", this.cid()) })
		this.sess.Pingack.Ack(msg)
		this.processAcked(this.sess.Pingack)

	case *message.DisconnectMessage:
		// For DISCONNECT message, we should quit
		this.sess.Cmsg.SetWillFlag(false)
		return errDisconnect

	default:
		return fmt.Errorf("(%s) invalid message type %s.", this.cid(), msg.Name())
	}

	if err != nil {
		Log.Error("(%s) Error processing acked message: %v", this.cid(), err)
	}

	return err
}

func (this *service) processAcked(ackq *sessions.Ackqueue) {
	for _, ackmsg := range ackq.Acked() {
		// Let's get the messages from the saved message byte slices.
		msg, err := ackmsg.Mtype.New()
		if err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("process/processAcked: Unable to creating new %s message: %v", ackmsg.Mtype, err)
			})
			continue
		}

		if _, err := msg.Decode(ackmsg.Msgbuf); err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("process/processAcked: Unable to decode %s message: %v", ackmsg.Mtype, err)
			})
			continue
		}

		ack, err := ackmsg.State.New()
		if err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("process/processAcked: Unable to creating new %s message: %v", ackmsg.State, err)
			})
			continue
		}

		if _, err := ack.Decode(ackmsg.Ackbuf); err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("process/processAcked: Unable to decode %s message: %v", ackmsg.State, err)
			})
			continue
		}

		//Log.Debugc(func() string{ return fmt.Sprintf("(%s) Processing acked message: %v", this.cid(), ack)})

		// - PUBACK if it's QoS 1 message. This is on the client side.
		// - PUBREL if it's QoS 2 message. This is on the server side.
		// - PUBCOMP if it's QoS 2 message. This is on the client side.
		// - SUBACK if it's a subscribe message. This is on the client side.
		// - UNSUBACK if it's a unsubscribe message. This is on the client side.
		switch ackmsg.State {
		case message.PUBREL:
			// If ack is PUBREL, that means the QoS 2 message sent by a remote client is
			// releassed, so let's publish it to other subscribers.
			if err = this.postPublish(msg.(*message.PublishMessage)); err != nil {
				Log.Errorc(func() string {
					return fmt.Sprintf("(%s) Error processing ack'ed %s message: %v", this.cid(), ackmsg.Mtype, err)
				})
			}

		case message.PUBACK, message.PUBCOMP, message.SUBACK, message.UNSUBACK, message.PINGRESP:
			// If ack is PUBACK, that means the QoS 1 message sent by this service got
			// ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is PUBCOMP, that means the QoS 2 message sent by this service got
			// ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is SUBACK, that means the SUBSCRIBE message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is UNSUBACK, that means the SUBSCRIBE message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is PINGRESP, that means the PINGREQ message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			err = nil

		default:
			Log.Errorc(func() string {
				return fmt.Sprintf("(%s) Invalid ack message type %s.", this.cid(), ackmsg.State)
			})
			continue
		}

		// Call the registered onComplete function
		if ackmsg.OnComplete != nil {
			onComplete, ok := ackmsg.OnComplete.(OnCompleteFunc)
			if !ok {
				Log.Errorc(func() string {
					return fmt.Sprintf("process/processAcked: Error type asserting onComplete function: %v", reflect.TypeOf(ackmsg.OnComplete))
				})
			} else if onComplete != nil {
				if err := onComplete(msg, ack, nil); err != nil {
					Log.Errorc(func() string {
						return fmt.Sprintf("process/processAcked: Error running onComplete(): %v", err)
					})
				}
			}
		}
	}
}

// For PUBLISH message, we should figure out what QoS it is and process accordingly
// If QoS == 0, we should just take the next step, no ack required
// If QoS == 1, we should send back PUBACK, then take the next step
// If QoS == 2, we need to put it in the ack queue, send back PUBREC
func (this *service) processPublish(msg *message.PublishMessage) error {
	switch msg.QoS() {
	case message.QosExactlyOnce:
		this.sess.Pub2in.Wait(msg, nil)

		resp := message.NewPubrecMessage()
		resp.SetPacketId(msg.PacketId())

		_, err := this.writeMessage(resp)

		err = this.preDispatchPublish(msg)
		return err

	case message.QosAtLeastOnce:
		resp := message.NewPubackMessage()
		resp.SetPacketId(msg.PacketId())

		if _, err := this.writeMessage(resp); err != nil {
			return err
		}

		err := this.preDispatchPublish(msg)
		return err
	case message.QosAtMostOnce:
		err := this.preDispatchPublish(msg)
		return err
	default:
		fmt.Printf("default: %d\n", msg.QoS())
	}

	return fmt.Errorf("(%s) invalid message QoS %d.", this.cid(), msg.QoS())
}

// For SUBSCRIBE message, we should add subscriber, then send back SUBACK
func (this *service) processSubscribe(msg *message.SubscribeMessage) error {
	resp := message.NewSubackMessage()
	resp.SetPacketId(msg.PacketId())

	// Subscribe to the different topics
	var retcodes []byte

	topics := msg.Topics()
	qos := msg.Qos()

	//   fmt.Printf("this.id: %d,  this.sess.ID(): %s\n", this.id, this.cid())
	for i, t := range topics {
		rqos, err := this.topicsMgr.Subscribe(t, qos[i], &this.onpub, this.sess.ID())
		//     rqos, err := this.topicsMgr.Subscribe(t, qos[i], &this)
		if err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("(%s) subscribe topic %s failed: %s", this.cid(), t, err)
			})
			this.stop()
			return err
		}
		Log.Infoc(func() string {
			return fmt.Sprintf("(%s) subscribe topic %s", this.cid(), t)
		})
		this.sess.AddTopic(string(t), qos[i])

		retcodes = append(retcodes, rqos)
	}

	if err := resp.AddReturnCodes(retcodes); err != nil {
		return err
	}

	if _, err := this.writeMessage(resp); err != nil {
		return err
	}

	for _, t := range topics {
		go this.pushOfflineMessage(string(t))
	}

	return nil
}

// For UNSUBSCRIBE message, we should remove the subscriber, and send back UNSUBACK
func (this *service) processUnsubscribe(msg *message.UnsubscribeMessage) error {
	topics := msg.Topics()

	for _, t := range topics {
		this.topicsMgr.Unsubscribe(t, &this.onpub)
		this.sess.RemoveTopic(string(t))
	}

	resp := message.NewUnsubackMessage()
	resp.SetPacketId(msg.PacketId())

	_, err := this.writeMessage(resp)
	return err
}

// 预投递publish类型的消息，如果是特殊频道特殊处理，否则正常处理
func (this *service) preDispatchPublish(msg *message.PublishMessage) (err error) {
	switch string(msg.Topic()) {
	case BroadCastChannel:
		go OnGroupPublish(msg, this)
	case SendChannel:
		go this.onReceiveBadge(msg)
	case ApnPushChannel:
		go onAPNsPush(msg, this)
	case OnlineStatusChannel:
		go this.checkOnlineStatus(msg)
	default:
		msg.SetPacketId(getNextPktId())
		Log.Infoc(func() string {
			return fmt.Sprintf("(%s) process private message.pkt_id: %d, payload size: %d", this.cid(), msg.PacketId(), len(msg.Payload()))
		})
		go this.postPublish(msg)
	}
	return
}

func (this *service) onReceiveBadge(msg *message.PublishMessage) (err error) {
	var badge_message BadgeMessage

	datas := strings.Split(string(msg.Payload()), ":")
	//   datas := strings.Split(fmt.Sprintf("%s", msg.Payload()), ":")
	if len(datas) != 2 {
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) invalid message payload: %s", this.cid(), msg.Payload())
		})
		return errors.New(fmt.Sprintf("invalid message payload: %s", msg.Payload()))
	}

	account_id := datas[0]
	payload_base64 := datas[1]

	if payload_base64 == "" {
		return errors.New(fmt.Sprintf("(%s) blank base64 payload, abort. %s", this.cid(), msg.Payload()))
	}

	payload_bytes, err := base64.StdEncoding.DecodeString(payload_base64)
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) can't decode payload: %s", this.cid(), payload_base64)
		})
	}

	err = ffjson.Unmarshal([]byte(payload_bytes), &badge_message)
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) can't parse badge json: account_id: %s, payload: %s", this.cid(), account_id, payload_bytes)
		})
		return
	}
	//   Log.Infoc(func() string{ return fmt.Sprintf("badge: %v, type: %T\n", badge_message.Data, badge_message.Data)})

	go this.processBadge(account_id, &badge_message)
	return
}

//根据指定ID查询客户端在线状态，并推送消息
func (this *service) checkOnlineStatus(msg *message.PublishMessage) {
	client_id := string(msg.Payload())
	online, lasttime, _ := GetOnlineStatus(client_id)

	payload := []byte(fmt.Sprintf(`{"client_id": "%s", "status": "%s", "since": "%s"}`, client_id, online, lasttime))

	msg.SetPayload(payload)
	this.postPublish(msg)
}

//根据topic和payload 推送消息
func (this *service) publishToTopic(topic string, payload []byte) {
	tmp_msg := _get_tmp_msg()
	tmp_msg.SetTopic([]byte(topic))
	tmp_msg.SetPayload(payload)

	Log.Debugc(func() string {
		return fmt.Sprintf("(%s) send msg %d to topic: %s", this.cid(), tmp_msg.PacketId(), topic)
	})
	this.postPublish(tmp_msg)
}

// 当某个topic被订阅，处理此topic所对应的、离线消息队列里的消息
func (this *service) pushOfflineMessage(topic string) (err error) {
	offline_msgs := this.getOfflineMsg(topic)
	if offline_msgs == nil {
		return nil
	}

	n := 0
	for _, payload := range offline_msgs {
		if payload != nil {
			this.publishToTopic(topic, payload)
			n++
		}
	}

	Log.Infoc(func() string {
		return fmt.Sprintf("(%s) send %d offline msgs to topic: %s", this.cid(), n, topic)
	})

	OfflineTopicCleanProcessor <- topic
	for i, _ := range offline_msgs {
		offline_msgs[i] = nil
	}
	//   TempBytes.Put(offline_msgs)
	return nil
}

// 获取一个递增的pkgid
func getNextPktId() uint16 {
	return (uint16)(atomic.AddUint32(&PktId, 1))
}

// processPublish() is called when the server receives a PUBLISH message AND have completed
// the ack cycle. This method will get the list of subscribers based on the publish
// topic, and publishes the message to the list of subscribers.
func (this *service) postPublish(msg *message.PublishMessage) (err error) {
	//   if msg.Retain() {
	//     if err = this.topicsMgr.Retain(msg); err != nil {
	//       Log.Errorc(func() string{ return fmt.Sprintf("(%s) Error retaining message: %v", this.cid(), err)})
	//     }
	//   }

	//   var subs []interface{}
	topic := string(msg.Topic())

	if !IsOnline(topic) {
		Log.Debugc(func() string {
			return fmt.Sprintf("(%s) this client is offline, send %d to offline queue.", this.cid(), msg.PacketId())
		})
		OfflineTopicQueueProcessor <- msg
		return nil
	}

	subs := _get_temp_subs()
	defer _return_temp_subs(subs)

	err = this.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &subs, nil)
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) Error retrieving subscribers list: %v", this.cid(), err)
		})
		return err
	}

	//   Log.Errorc(func() string{ return fmt.Sprintf("(%s) Publishing to topic %q and %d subscribers", this.cid(), string(msg.Topic()), len(this.subs))})
	//   fmt.Printf("value: %v\n", config.GetModel())
	//   done := make(chan bool)
	pending_status := NewPendingStatus(topic, msg)
	pkt_id := msg.PacketId()
	PendingQueue[pkt_id] = pending_status

	go this.handlePendingMessage(msg, pending_status)

	for _, s := range subs {
		if s != nil {
			fn, ok := s.(*OnPublishFunc)
			if !ok {
				Log.Errorc(func() string {
					return fmt.Sprintf("Invalid onPublish Function: %T", s)
				})
				return fmt.Errorf("Invalid onPublish Function")
			} else {
				(*fn)(msg)
				//         Log.Errorc(func() string{ return fmt.Sprintf("OfflineTopicQueue[%s]: %v, len is: %d\n", msg.Topic(), OfflineTopicQueue[string(msg.Topic())], len(OfflineTopicQueue[string(msg.Topic())]))})
			}
		}
	}

	return nil
}

// 处理苹果设备的未读数，修改redis
func (this *service) processBadge(account_id string, badge_message *BadgeMessage) {
	key := "badge_account:" + account_id
	_, err := topics.RedisDo("set", key, badge_message.Data)
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) can't set badge! account_id: %s, badge: %v", this.cid(), account_id, badge_message)
		})
	}
}

// 根据topic获取离线消息队列
func (this *service) getOfflineMsg(topic string) (msgs [][]byte) {
	OfflineTopicRWmux.RLock()
	q := OfflineTopicMap[topic]
	OfflineTopicRWmux.RUnlock()

	if q == nil {
		msgs = nil
	} else {
		msgs = q.GetAll()
	}

	// 去pending队列里找该topic的，append进来，但可能还是会有时间差
	n := 0
	for i, pmsg := range PendingQueue {
		if pmsg != nil && pmsg.Topic == topic && pmsg.Done == nil {
			msgs = append(msgs, pmsg.Msg.Payload())

			select {
			case pmsg.Done <- true:
				n++
				// 直接打断pending状态
			default:
				//说明有问题，只有两种情况： 堵死或者nil。
				PendingQueue[i] = nil
				Log.Errorc(func() string {
					return fmt.Sprintf("(%s) send done to pending failed. msg: %v", this.cid(), pmsg)
				})
			}

		}
	}

	if n > 0 {
		Log.Debugc(func() string {
			return fmt.Sprintf("(%s) append %d pending msg to offline queue msgs.", this.cid(), n)
		})
	}
	return msgs
}

func (this *service) retryPublish(msg *message.PublishMessage) (err error) {
	//   if msg.Retain() {
	//     if err = this.topicsMgr.Retain(msg); err != nil {
	//       Log.Errorc(func() string{ return fmt.Sprintf("(%s) Error retaining message: %v", this.cid(), err)})
	//     }
	//   }

	//   var subs []interface{}
	topic := string(msg.Topic())

	subs := _get_temp_subs()
	defer _return_temp_subs(subs)

	err = this.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &subs, nil)
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) Error retrieving subscribers list: %v", this.cid(), err)
		})
		return err
	}

	pending_status := NewPendingStatus(topic, msg)
	pkt_id := msg.PacketId()
	PendingQueue[pkt_id] = pending_status

	for _, s := range subs {
		if s != nil {
			fn, ok := s.(*OnPublishFunc)
			if !ok {
				Log.Errorc(func() string {
					return fmt.Sprintf("Invalid onPublish Function: %T", s)
				})
				return fmt.Errorf("Invalid onPublish Function")
			} else {
				(*fn)(msg)
			}
		}
	}

	return nil
}

// 判断消息是否已读
func (this *service) handlePendingMessage(msg *message.PublishMessage, pending_status *PendingStatus) {
	// 如果QOS=0,则无需等待直接返回
	if string(msg.Topic()) == OnlineStatusChannel {
		return
	}

	// 将msg按照pkt_id，存入pending队列
	pkt_id := msg.PacketId()
	//   pending_msg := NewPendingMessage(msg)

	select {
	case <-pending_status.Done:
	// 消息已成功接收，不再等待
	case <-time.After(time.Second * MsgPendingTime):
		// 重试一次
		Log.Debugc(func() string {
			return fmt.Sprintf("(%s) receive ack %d timeout. try to resend. topic: %s", this.cid(), msg.PacketId(), msg.Topic())
		})
		this.retryPublish(msg)

		select {
		case <-pending_status.Done:
			// 重发的消息已成功接收，不再等待
		case <-time.After(time.Second * MsgPendingTime):
			// 没有回ack，放到离线队列里
			Log.Debugc(func() string {
				return fmt.Sprintf("(%s) receive ack %d timeout. send msg to offline msg queue.topic: %s", this.cid(), msg.PacketId(), msg.Topic())
				//       return fmt.Sprintf("(%s) receive ack timeout. send msg to offline msg queue.topic: %s, payload: %s", this.cid(), msg.Topic(),msg.Payload())
			})
			OfflineTopicQueueProcessor <- msg
		}
	}
	PendingQueue[pkt_id] = nil
}

// 从池子里获取一个长度为1的slice，用于填充订阅队列
func _get_temp_subs() (subs []interface{}) {
	/*
		select {
		case subs = <-SubscribersSliceQueue:
		// 成功从缓存池里拿到，直接返回
		default:
			// 拿不到，说明池子里没对象了，就地创建一个
			sub_p := make([]interface{}, 1, 1)
			return sub_p
		}
	*/

	return p.Get().([]interface{})
}

// 把subs返还池子
func _return_temp_subs(subs []interface{}) {
	/*
		subs[0] = nil
		select {
		case SubscribersSliceQueue <- subs:
		// 成功返还，什么都不做
		default:
		}
	*/
	subs[0] = nil
	p.Put(subs)
}

// 从池子里获取一个msg对象，用于打包
func _get_tmp_msg() (msg *message.PublishMessage) {
	/*
		select {
		case msg = <-NewMessagesQueue:
			msg.SetPacketId(GetNextPktId())
		// 成功取到msg，什么都不做
		default:
			Log.Debugc(func() string {
				return "no tmp msg in NewMsgQueue. will new it."
			})
			msg = message.NewPublishMessage()
			msg.SetQoS(message.QosAtLeastOnce)
			msg.SetPacketId(GetNextPktId())
		}
	*/

	for {
		msg = MessagePool.Get().(*message.PublishMessage)
		if msg != nil {
			msg.SetPacketId(getNextPktId())
			return
		}
	}
}

func _return_tmp_msg(msg *message.PublishMessage) {
	/*
		select {
		case NewMessagesQueue <- msg:
		//成功还回去了，什么都不做
		default:
		}
	*/
	//   mp.Put(msg)
	select {
	case OldMessagesQueue <- msg:
	default:
	}
}

type BroadCastMessage struct {
	Clients []string `json:"clients"`
	Payload string   `json:"payload"`
}
type MtBroadCastMessage struct {
	Clients []string `json:"topics"`
	Payload string   `json:"payload"`
}
type BadgeMessage struct {
	Data int    `json:data`
	Type string `json:type`
}

package topics

import (
	"fmt"
	//   "github.com/garyburd/redigo/redis"
	//   "github.com/nagae-memooff/config"
	//   "github.com/nagae-memooff/surgemq/topics"
	//   "github.com/nagae-memooff/surgemq/service"
	"github.com/surgemq/message"
	"sync"
)

var (
// MXMaxQosAllowed is the maximum QOS supported by this server
//   MXMaxQosAllowed = message.QosAtLeastOnce
//   RedisPool       *redis.Pool
//   Channelcache    map[string]string
)

var _ TopicsProvider = (*mtTopics)(nil)

type mtTopics struct {
	// Sub/unsub mutex
	smu sync.RWMutex
	// Subscription tree
	//   sroot *mxsnode

	// subscription map
	//实际类型应该是： map[string]*onPublishFunc
	subscriber map[string]interface{}
}

func init() {
	Register("mt", NewMTProvider())
}

// NewMemProvider returns an new instance of the mtTopics, which is implements the
// TopicsProvider interface. memProvider is a hidden struct that stores the topic
// subscriptions and retained messages in memory. The content is not persistend so
// when the server goes, everything will be gone. Use with care.
func NewMTProvider() *mtTopics {
	return &mtTopics{
		//     sroot: newMXSNode(),
		subscriber: make(map[string]interface{}),
	}
}

func (this *mtTopics) Subscribe(topic []byte, qos byte, sub interface{}, client_id string) (byte, error) {
	topic_str := string(topic)
	if !message.ValidQos(qos) {
		return message.QosFailure, fmt.Errorf("Invalid QoS %d", qos)
	}

	if sub == nil {
		return message.QosFailure, fmt.Errorf("Subscriber cannot be nil")
	}

	if qos > MXMaxQosAllowed {
		//     Log.Printf("invalid qos: %d\n", qos)
		qos = MXMaxQosAllowed
	}
	//   Log.Errorc(func() string{ return fmt.Sprintf("topic: %s, qos: %d,  client_id: %s\n", topic, qos, client_id)})

	this.smu.Lock()
	this.subscriber[topic_str] = sub
	this.smu.Unlock()

	return qos, nil
}

func (this *mtTopics) Unsubscribe(topic []byte, sub interface{}) error {
	this.smu.Lock()
	this.subscriber[string(topic)] = nil
	this.smu.Unlock()

	return nil
}

// Returned values will be invalidated by the next Subscribers call
func (this *mtTopics) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	if !message.ValidQos(qos) {
		return fmt.Errorf("Invalid QoS %d", qos)
	}

	this.smu.RLock()
	(*subs)[0] = this.subscriber[string(topic)]
	this.smu.RUnlock()
	//   *qoss = (*qoss)[0:0]
	return nil
}

func (this *mtTopics) Retain(msg *message.PublishMessage) error {
	return nil
}

func (this *mtTopics) Retained(topic []byte, msgs *[]*message.PublishMessage) error {
	return nil
}

func (this *mtTopics) Close() error {
	for key, _ := range this.subscriber {
		delete(this.subscriber, key)
	}

	this.subscriber = nil
	return nil
}

package topics

import (
	"fmt"
	//   "github.com/nagae-memooff/config"
	//   "github.com/nagae-memooff/surgemq/topics"
	//   "github.com/nagae-memooff/surgemq/service"
	"github.com/surgemq/message"
	"strings"
	"sync"
)

var (
// MXMaxQosAllowed is the maximum QOS supported by this server
)

var _ TopicsProvider = (*mxTopics)(nil)

type mxTopics struct {
	// Sub/unsub mutex
	smu sync.RWMutex

	// subscription map
	// 实际类型应该是： map[string]*onPublishFunc
	subscriber map[string]interface{}
}

func init() {
	Register("mx", NewMXProvider())
}

// NewMemProvider returns an new instance of the mxTopics, which is implements the
// TopicsProvider interface. memProvider is a hidden struct that stores the topic
// subscriptions and retained messages in memory. The content is not persistend so
// when the server goes, everything will be gone. Use with care.
func NewMXProvider() *mxTopics {
	return &mxTopics{
		//     sroot: newMXSNode(),
		subscriber: make(map[string]interface{}),
	}
}

func (this *mxTopics) Subscribe(topic []byte, qos byte, sub interface{}, client_id string) (byte, error) {
	if !message.ValidQos(qos) {
		return message.QosFailure, fmt.Errorf("Invalid QoS %d", qos)
	}

	if sub == nil {
		return message.QosFailure, fmt.Errorf("Subscriber cannot be nil")
	}

	topic_str := string(topic)
	if !checkValidchannel(client_id, topic_str) {
		return message.QosFailure, fmt.Errorf("%s: Invalid Channel %s", client_id, topic_str)
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

func (this *mxTopics) Unsubscribe(topic []byte, sub interface{}) error {
	this.smu.Lock()
	this.subscriber[string(topic)] = nil
	this.smu.Unlock()

	return nil
}

// Returned values will be invalidated by the next Subscribers call
func (this *mxTopics) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	if !message.ValidQos(qos) {
		return fmt.Errorf("Invalid QoS %d", qos)
	}

	this.smu.RLock()
	(*subs)[0] = this.subscriber[string(topic)]
	this.smu.RUnlock()
	//   *qoss = (*qoss)[0:0]
	return nil
}

func (this *mxTopics) Retain(msg *message.PublishMessage) error {
	return nil
}

func (this *mxTopics) Retained(topic []byte, msgs *[]*message.PublishMessage) error {
	return nil
}

func (this *mxTopics) Close() error {
	for key, _ := range this.subscriber {
		delete(this.subscriber, key)
	}

	this.subscriber = nil
	return nil
}

func checkValidchannel(client_id, topic string) bool {
	if client_id == "master_8859" {
		//     Log.Printf("%s, %s",client_id, topic)
		return true
	} else if strings.HasPrefix(client_id, "/apn/invalid_tokens") {
		return true
	} else if GetUserTopic(client_id) == topic {
		//     Log.Printf("%v, %s", data, err)
		return true
	}
	return false
}

func GetUserTopic(client_id string) (topic string) {
	//   defer fmt.Printf("%s get topic: %s\n", client_id, topic)
	Cmux.RLock()
	topic = Channelcache[client_id]
	Cmux.RUnlock()
	if topic != "" {
		return
	}

	key := "channel:" + client_id
	data, err := RedisDo("get", key)
	if err != nil {
		fmt.Println(err)
		topic = ""
		return
	}

	if data == "" {
		topic = ""
		return
	}

	topic = "/u/" + data
	Cmux.Lock()
	Channelcache[client_id] = topic
	ChannelReversecache[topic] = client_id
	Cmux.Unlock()
	return
}

func LoadChannelCache() {
	client_ids, channel_keys, err := GetClientIDandChannels()
	if err != nil {
		fmt.Println(err)
		return
	}

	channels, err := RedisDoGetMulti("mget", channel_keys...)
	if err != nil {
		fmt.Println(err)
		return
	}

	if len(channels) != len(channel_keys) {
		fmt.Println("长度不一致，不缓存了！")
	} else {
		for i := 0; i < len(channels); i++ {
			topic := "/u/" + channels[i]
			client_id := client_ids[i]

			Channelcache[client_id] = topic
			ChannelReversecache[topic] = client_id
		}
	}
}

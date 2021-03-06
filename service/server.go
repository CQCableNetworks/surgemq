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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	//   "net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//"encoding/binary"
	"github.com/nagae-memooff/config"
	log "github.com/nagae-memooff/log4go"
	"github.com/nagae-memooff/surgemq/auth"
	"github.com/nagae-memooff/surgemq/sessions"
	"github.com/nagae-memooff/surgemq/topics"
	"github.com/surgemq/message"
)

var (
	Log      log.Logger
	LogLevel log.Level

	ErrInvalidConnectionType  error = errors.New("service: Invalid connection type")
	ErrInvalidSubscriber      error = errors.New("service: Invalid subscriber")
	ErrBufferNotReady         error = errors.New("service: buffer is not ready")
	ErrBufferInsufficientData error = errors.New("service: buffer has insufficient data.")
)

const (
	DefaultKeepAlive        = 300
	DefaultConnectTimeout   = 2
	DefaultAckTimeout       = 20
	DefaultTimeoutRetries   = 3
	DefaultSessionsProvider = "mem"
	DefaultAuthenticator    = "mockSuccess"
	DefaultTopicsProvider   = "mx"
)

// Server is a library implementation of the MQTT server that, as best it can, complies
// with the MQTT 3.1 and 3.1.1 specs.
type Server struct {
	// The number of seconds to keep the connection live if there's no data.
	// If not set then default to 5 mins.
	KeepAlive int

	// The number of seconds to wait for the CONNECT message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// The number of seconds to wait for any ACK messages before failing.
	// If not set then default to 20 seconds.
	AckTimeout int

	// The number of times to retry sending a packet if ACK is not received.
	// If no set then default to 3 retries.
	TimeoutRetries int

	// Authenticator is the authenticator used to check username and password sent
	// in the CONNECT message. If not set then default to "mockSuccess".
	Authenticator string

	// SessionsProvider is the session store that keeps all the Session objects.
	// This is the store to check if CleanSession is set to 0 in the CONNECT message.
	// If not set then default to "mem".
	SessionsProvider string

	// TopicsProvider is the topic store that keeps all the subscription topics.
	// If not set then default to "mx".
	TopicsProvider string

	// authMgr is the authentication manager that we are going to use for authenticating
	// incoming connections
	authMgr *auth.Manager

	// sessMgr is the sessions manager for keeping track of the sessions
	sessMgr *sessions.Manager

	// topicsMgr is the topics manager for keeping track of subscriptions
	topicsMgr *topics.Manager

	// The quit channel for the server. If the server detects that this channel
	// is closed, then it's a signal for it to shutdown as well.
	quit chan struct{}

	ln      net.Listener
	ssl_ln  net.Listener
	japn_ln net.Listener

	// A list of services created by the server. We keep track of them so we can
	// gracefully shut them down if they are still alive when the server goes down.
	svcs []*service

	// Mutex for updating svcs
	mu sync.Mutex

	// A indicator on whether this server is running
	running int32

	// A indicator on whether this server has already checked configuration
	configOnce sync.Once

	subs []interface{}
	qoss []byte

	arrayPool *sync.Pool
}

func (this *Server) CreateAndGetBytes(size int64) []byte {
	/*b := this.arrayPool.Get().([]byte)
	if size > int64(len(b)) {
		b = nil
		return make([]byte, size)
	}
	return b*/
	return make([]byte, size)
}

func (this *Server) DestoryBytes(b []byte) {
	/*this.arrayPool.Put(b)*/
	b = nil
}

func (this *Server) getRealBytes(p *[]byte) []byte {
	/*max_cnt := 1
	b := this.CreateAndGetBytes(int64(5))
	for {
		// If we have read 5 bytes and still not done, then there's a problem.
		if max_cnt > 4 {
			Log.Debugc(func() string {
				return fmt.Sprintf("Server/getRealBytes: 4th byte of remaining length has continuation bit set.")
			})
			return nil
		}
		copy(b[max_cnt:(max_cnt + 1)], (*p)[max_cnt:(max_cnt + 1)])

		if b[max_cnt] >= 0x80 {
			max_cnt++
		} else {
			break
		}
	}
	remlen, m := binary.Uvarint(b[1 : max_cnt + 1])
	remlen_tmp := int64(remlen)
	start_ := int64(1) + int64(m)
	total_tmp := remlen_tmp + start_
	this.DestoryBytes(b)
	_p := (*p)[0:total_tmp]
	return _p*/
	return *p
}

// ListenAndServe listents to connections on the URI requested, and handles any
// incoming MQTT client sessions. It should not return until Close() is called
// or if there's some critical error that stops the server from running. The URI
// supplied should be of the form "protocol://host:port" that can be parsed by
// url.Parse(). For example, an URI could be "tcp://0.0.0.0:1883".
func (this *Server) ListenAndServe() error {
	defer atomic.CompareAndSwapInt32(&this.running, 1, 0)

	this.arrayPool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, 50)
		},
	}

	if !atomic.CompareAndSwapInt32(&this.running, 0, 1) {
		Log.Error("server/ListenAndServe: Server is already running")
		return fmt.Errorf("server/ListenAndServe: Server is already running")
	}

	this.quit = make(chan struct{})

	//     _, err := url.Parse(uri)
	if this.TopicsProvider == "mx" {
		OnGroupPublish = mxOnGroupPublish

		//根据pkt_id，将pending队列里的该条消息移除
		processAck = mxProcessAck

		IsOnline = mxIsOnline

		if config.GetBool("apn_always_reconnect") {
			onAPNsPush = mxAPNsPush2
		} else {
			onAPNsPush = mxAPNsPush
		}

	} else if this.TopicsProvider == "mt" {

		OnGroupPublish = mtOnGroupPublish

		processAck = mtProcessAck

		IsOnline = mtIsOnline

		onAPNsPush = mtAPNsPush
	}

	var err error

	if strings.Contains(config.Get("uri_scheme"), "ssl") {
		go func() {
			cer, err := tls.LoadX509KeyPair(config.Get("ssl_cert"), config.Get("ssl_key"))
			if err != nil {
				Log.Error(err)
				panic(err)
			}
			tls_config := &tls.Config{Certificates: []tls.Certificate{cer}}
			ssl_host := fmt.Sprintf("%s:%s", config.GetMulti("ssl_listen_addr", "ssl_port")...)
			this.ssl_ln, err = tls.Listen("tcp", ssl_host, tls_config)
			if err != nil {
				Log.Error(err)
				panic(err)
			}
			Log.Info("listening ssl: %v", ssl_host)

			defer this.ssl_ln.Close()
			var tempDelay time.Duration // how long to sleep on accept failure

			for {
				conn, err := this.ssl_ln.Accept()
				atomic_id := atomic.AddUint64(&gsvcid, 1)

				Log.Debugc(func() string {
					var raddr string
					if conn == nil {
						raddr = "client but the conn is already Destroyed."
					} else {
						raddr = conn.RemoteAddr().String()
					}
					return fmt.Sprintf("(%d/_) accepted ssl connection from %s", atomic_id, raddr)
				})
				if err != nil {
					// Borrowed from go1.3.3/src/pkg/net/http/server.go:1699
					if ne, ok := err.(net.Error); ok && ne.Temporary() {
						if tempDelay == 0 {
							tempDelay = 5 * time.Millisecond
						} else {
							tempDelay *= 2
						}
						if max := 1 * time.Second; tempDelay > max {
							tempDelay = max
						}
						Log.Error("(%d/_) server/ListenAndServe: Accept ssl error: %v; retrying in %v", atomic_id, err, tempDelay)
						time.Sleep(tempDelay)
						continue
					} else {
						Log.Errorc(func() string {
							return fmt.Sprintf("(%d/_) ssl connection error: %s", atomic_id, err)
						})
					}

					return
				}

				go this.handleConnection(conn, atomic_id)
			}
		}()
	}

	if strings.Contains(config.Get("uri_scheme"), "tcp") {
		go func() {
			tcp_host := fmt.Sprintf("%s:%s", config.GetMulti("tcp_listen_addr", "tcp_port")...)

			this.ln, err = net.Listen("tcp", tcp_host)
			if err != nil {
				Log.Error(err)
				panic(err)
				//         return
			}
			Log.Info("listening tcp: %v", tcp_host)

			defer this.ln.Close()
			var tempDelay time.Duration // how long to sleep on accept failure

			for {
				conn, err := this.ln.Accept()
				atomic_id := atomic.AddUint64(&gsvcid, 1)

				Log.Debugc(func() string {
					var raddr string
					if conn == nil {
						raddr = "client but the conn is already Destroyed."
					} else {
						raddr = conn.RemoteAddr().String()
					}
					return fmt.Sprintf("(%d/_) accepted tcp connection from %s", atomic_id, raddr)
				})

				if err != nil {
					// Borrowed from go1.3.3/src/pkg/net/http/server.go:1699
					if ne, ok := err.(net.Error); ok && ne.Temporary() {
						if tempDelay == 0 {
							tempDelay = 5 * time.Millisecond
						} else {
							tempDelay *= 2
						}
						if max := 1 * time.Second; tempDelay > max {
							tempDelay = max
						}
						Log.Error("(%d/_) server/ListenAndServe: Accept error: %v; retrying in %v", atomic_id, err, tempDelay)
						time.Sleep(tempDelay)
						continue
					} else {
						Log.Errorc(func() string {
							return fmt.Sprintf("(%d/_) tcp connection error: %s", atomic_id, err)
						})
					}
					return
				}

				go this.handleConnection(conn, atomic_id)
			}
		}()

	}

	if config.GetBool("japn_compatibility") {
		go func() {
			tcp_host := "0.0.0.0:2884"

			this.japn_ln, err = net.Listen("tcp", tcp_host)
			if err != nil {
				Log.Error(err)
				panic(err)
				//         return
			}
			Log.Info("listening tcp: %v", tcp_host)

			defer this.japn_ln.Close()
			var tempDelay time.Duration // how long to sleep on accept failure

			for {
				conn, err := this.japn_ln.Accept()
				atomic_id := atomic.AddUint64(&gsvcid, 1)

				Log.Debugc(func() string {
					var raddr string
					if conn == nil {
						raddr = "client but the conn is already Destroyed."
					} else {
						raddr = conn.RemoteAddr().String()
					}
					return fmt.Sprintf("(%d/_) accepted japn_compatibility connection from %s", atomic_id, raddr)
				})

				if err != nil {
					// Borrowed from go1.3.3/src/pkg/net/http/server.go:1699
					if ne, ok := err.(net.Error); ok && ne.Temporary() {
						if tempDelay == 0 {
							tempDelay = 5 * time.Millisecond
						} else {
							tempDelay *= 2
						}
						if max := 1 * time.Second; tempDelay > max {
							tempDelay = max
						}
						Log.Error("(%d/_) server/ListenAndServe: Accept error: %v; retrying in %v", atomic_id, err, tempDelay)
						time.Sleep(tempDelay)
						continue
					}
					return
				}

				go this.handleConnection(conn, atomic_id)
			}
		}()

	}

	<-this.quit
	return nil
}

// Publish sends a single MQTT PUBLISH message to the server. On completion, the
// supplied OnCompleteFunc is called. For QOS 0 messages, onComplete is called
// immediately after the message is sent to the outgoing buffer. For QOS 1 messages,
// onComplete is called when PUBACK is received. For QOS 2 messages, onComplete is
// called after the PUBCOMP message is received.
func (this *Server) Publish(msg *message.PublishMessage, onComplete OnCompleteFunc) (err error) {
	if err = this.checkConfiguration(); err != nil {
		return err
	}

	if err = this.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &this.subs, &this.qoss); err != nil {
		//   if err = this.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &this.subs, &this.qoss); err != nil {
		return err
	}

	subs := _get_temp_subs()
	defer _return_temp_subs(subs)
	//   defer _return_tmp_msg(msg)

	Log.Debugc(func() string {
		return fmt.Sprintf("(server) Publishing to topic %s and %d subscribers", string(msg.Topic()), len(subs))
	})

	for _, s := range subs {
		if s != nil {
			fn, ok := s.(*OnPublishFunc)
			if !ok {
				Log.Error("Invalid onPublish Function")
			} else {
				err = (*fn)(msg)
			}
		}
	}

	return err
}

// Close terminates the server by shutting down all the client connections and closing
// the listener. It will, as best it can, clean up after itself.
func (this *Server) Close() error {
	// By closing the quit channel, we are telling the server to stop accepting new
	// connection.
	close(this.quit)

	// We then close the net.Listener, which will force Accept() to return if it's
	// blocked waiting for new connections.
	if this.ln != nil {
		this.ln.Close()
	}

	if this.ssl_ln != nil {
		this.ssl_ln.Close()
	}

	if this.japn_ln != nil {
		this.japn_ln.Close()
	}

	for _, svc := range this.svcs {
		Log.Infoc(func() string {
			return fmt.Sprintf("Stopping service %d", svc.id)
		})
		svc.stop()
	}

	if this.sessMgr != nil {
		this.sessMgr.Close()
	}

	if this.topicsMgr != nil {
		this.topicsMgr.Close()
	}

	return nil
}

// HandleConnection is for the broker to handle an incoming connection from a client
func (this *Server) handleConnection(c io.Closer, atomic_id uint64) (err error) {
	if c == nil {
		return ErrInvalidConnectionType
	}

	defer func() {
		if err != nil {
			c.Close()
		}
	}()

	err = this.checkConfiguration()
	if err != nil {
		return err
	}

	conn, ok := c.(net.Conn)
	if !ok {
		return ErrInvalidConnectionType
	}

	// To establish a connection, we must
	// 1. Read and decode the message.ConnectMessage from the wire
	// 2. If no decoding errors, then authenticate using username and password.
	//    Otherwise, write out to the wire message.ConnackMessage with
	//    appropriate error.
	// 3. If authentication is successful, then either create a new session or
	//    retrieve existing session
	// 4. Write out to the wire a successful message.ConnackMessage message

	// Read the CONNECT message from the wire, if error, then check to see if it's
	// a CONNACK error. If it's CONNACK error, send the proper CONNACK error back
	// to client. Exit regardless of error type.

	conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(this.ConnectTimeout)))

	resp := message.NewConnackMessage()

	req, err := getConnectMessage(conn)
	if err != nil {
		if cerr, ok := err.(message.ConnackCode); ok {
			//Log.Debugc(func() string{ return fmt.Sprintf("request   message: %s\nresponse message: %s\nerror           : %v", mreq, resp, err)})
			resp.SetReturnCode(cerr)
			resp.SetSessionPresent(false)
			writeMessage(conn, resp)
		}
		return err
	}

	c_id := string(req.ClientId())
	c_hash := ClientHash{Name: c_id, Conn: &conn}

	// Authenticate the user, if error, return error and exit
	auth_info := auth.NewAuthInfo(req.Password(), req.ClientId(), atomic_id)
	if err = this.authMgr.Authenticate(string(req.Username()), auth_info); err != nil {
		Log.Infoc(func() string {
			return fmt.Sprintf("(%d/%s) client auth failed. username: %s, client_id: %s", atomic_id, c_id, req.Username(), c_id)
		})
		resp.SetReturnCode(message.ErrBadUsernameOrPassword)
		resp.SetSessionPresent(false)
		writeMessage(conn, resp)
		time.Sleep(3 * time.Second)
		return err
	}

	if req.KeepAlive() == 0 {
		req.SetKeepAlive(minKeepAlive)
	}

	svc := &service{
		id:     atomic_id,
		client: false,

		keepAlive:      int(req.KeepAlive()),
		connectTimeout: this.ConnectTimeout,
		ackTimeout:     this.AckTimeout,
		timeoutRetries: this.TimeoutRetries,

		conn:      conn,
		sessMgr:   this.sessMgr,
		topicsMgr: this.topicsMgr,
		server:    this,
	}

	ClientMapProcessor <- c_hash

	err = this.getSession(svc, req, resp)
	if err != nil {
		return err
	}

	resp.SetReturnCode(message.ConnectionAccepted)

	if err = writeMessage(c, resp); err != nil {
		return err
	}

	svc.inStat.increment(int64(req.Len()))
	svc.outStat.increment(int64(resp.Len()))

	if err := svc.start(c_id); err != nil {
		svc.stop()
		return err
	}

	//this.mu.Lock()
	//this.svcs = append(this.svcs, svc)
	//this.mu.Unlock()

	Log.Debugc(func() string {
		return fmt.Sprintf("(%s) client connected successfully.", svc.cid())
	})

	return nil
}

func (this *Server) checkConfiguration() error {
	var err error

	this.configOnce.Do(func() {
		if this.KeepAlive == 0 {
			this.KeepAlive = DefaultKeepAlive
		}

		if this.ConnectTimeout == 0 {
			this.ConnectTimeout = DefaultConnectTimeout
		}

		if this.AckTimeout == 0 {
			this.AckTimeout = DefaultAckTimeout
		}

		if this.TimeoutRetries == 0 {
			this.TimeoutRetries = DefaultTimeoutRetries
		}

		if this.Authenticator == "" {
			this.Authenticator = "mockSuccess"
		}

		this.authMgr, err = auth.NewManager(this.Authenticator)
		if err != nil {
			return
		}

		if this.SessionsProvider == "" {
			this.SessionsProvider = "mem"
		}

		this.sessMgr, err = sessions.NewManager(this.SessionsProvider)
		if err != nil {
			return
		}

		if this.TopicsProvider == "" {
			this.TopicsProvider = "mx"
		}

		this.topicsMgr, err = topics.NewManager(this.TopicsProvider)

		return
	})

	return err
}

func (this *Server) getSession(svc *service, req *message.ConnectMessage, resp *message.ConnackMessage) error {
	// If CleanSession is set to 0, the server MUST resume communications with the
	// client based on state from the current session, as identified by the client
	// identifier. If there is no session associated with the client identifier the
	// server must create a new session.
	//
	// If CleanSession is set to 1, the client and server must discard any previous
	// session and start a new one. This session lasts as long as the network c
	// onnection. State data associated with this session must not be reused in any
	// subsequent session.

	var err error

	// Check to see if the client supplied an ID, if not, generate one and set
	// clean session.
	if len(req.ClientId()) == 0 {
		req.SetClientId([]byte(fmt.Sprintf("internalclient%d", svc.id)))
		req.SetCleanSession(true)
	}

	cid := string(req.ClientId())

	// If CleanSession is NOT set, check the session store for existing session.
	// If found, return it.
	// if !req.CleanSession() {
	// 	if svc.sess, err = this.sessMgr.Get(cid); err == nil {
	// 		resp.SetSessionPresent(true)
	//
	// 		if err := svc.sess.Update(req); err != nil {
	// 			return err
	// 		}
	// 	}
	// }

	//   this.sessMgr.Del(cid)

	// If CleanSession, or no existing session found, then create a new one
	if svc.sess == nil {
		if svc.sess, err = this.sessMgr.New(cid); err != nil {
			return err
		}

		resp.SetSessionPresent(false)

		if err := svc.sess.Init(req); err != nil {
			return err
		}
	}

	return nil
}

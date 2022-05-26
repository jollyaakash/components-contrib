/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mqtte4k

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/jwtsvid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
)

const (
	// Keys.
	mqttURL               = "url"
	mqttQOS               = "qos"
	mqttRetain            = "retain"
	mqttClientID          = "consumerID"
	mqttCleanSession      = "cleanSession"
	mqttCACert            = "caCert"
	mqttClientCert        = "clientCert"
	mqttClientKey         = "clientKey"
	mqttBackOffMaxRetries = "backOffMaxRetries"
	mqttKeepAliveDuration = "keepAlive"

	// errors.
	errorMsgPrefix = "mqtte4k pub sub error:"

	// Defaults.
	defaultQOS               = 0
	defaultRetain            = false
	defaultWait              = 3 * time.Second
	defaultCleanSession      = true
	defaultKeepAliveDuration = 1000
	defaultSpiffeSocketPath  = "/run/iotedge/sockets/workloadapi.sock"

	// Spiffe keys.
	spiffeSocketPath = "spiffeSocketPath"
)

// mqttPubSub type allows sending and receiving data to/from MQTT broker.
type mqttPubSub struct {
	client   mqtt.Client
	metadata *metadata
	logger   logger.Logger
	topics   map[string]byte

	ctx     context.Context
	cancel  context.CancelFunc
	backOff backoff.BackOff
	svid    *jwtsvid.SVID
}

// NewMQTTPubSub returns a new mqttPubSub instance.
func NewMQTTPubSub(logger logger.Logger) pubsub.PubSub {
	return &mqttPubSub{logger: logger}
}

func parseMQTTMetaData(md pubsub.Metadata) (*metadata, error) {
	m := metadata{}

	// required configuration settings
	if val, ok := md.Properties[mqttURL]; ok && val != "" {
		m.url = val
	} else {
		return &m, fmt.Errorf("%s missing url", errorMsgPrefix)
	}

	// optional configuration settings
	m.qos = defaultQOS
	if val, ok := md.Properties[mqttQOS]; ok && val != "" {
		qosInt, err := strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid qos %s, %s", errorMsgPrefix, val, err)
		}
		m.qos = byte(qosInt)
	}

	m.retain = defaultRetain
	if val, ok := md.Properties[mqttRetain]; ok && val != "" {
		var err error
		m.retain, err = strconv.ParseBool(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid retain %s, %s", errorMsgPrefix, val, err)
		}
	}

	if val, ok := md.Properties[mqttClientID]; ok && val != "" {
		m.clientID = val
	} else {
		return &m, fmt.Errorf("%s missing consumerID", errorMsgPrefix)
	}

	m.cleanSession = defaultCleanSession
	if val, ok := md.Properties[mqttCleanSession]; ok && val != "" {
		var err error
		m.cleanSession, err = strconv.ParseBool(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid clean session %s, %s", errorMsgPrefix, val, err)
		}
	}

	if val, ok := md.Properties[mqttBackOffMaxRetries]; ok && val != "" {
		backOffMaxRetriesInt, err := strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid backOffMaxRetries %s, %s", errorMsgPrefix, val, err)
		}
		m.backOffMaxRetries = backOffMaxRetriesInt
	}

	m.keepAliveDuration = defaultKeepAliveDuration
	if val, ok := md.Properties[mqttKeepAliveDuration]; ok && val != "" {
		keepAliveDurationInt, err := strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid keepAliveDuration %s, %s", errorMsgPrefix, val, err)
		}
		m.keepAliveDuration = keepAliveDurationInt
	}

	// optional configuration settings
	m.spiffeSocketPath = defaultSpiffeSocketPath
	if val, ok := md.Properties[spiffeSocketPath]; ok && val != "" {
		m.spiffeSocketPath = val
	} else {
		return &m, fmt.Errorf("%s Invalid or Missing spiffeSocketPath", errorMsgPrefix)
	}

	return &m, nil
}

func initSpiffeWorkloadApi(m *mqttPubSub) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	serverID := spiffeid.RequireFromString("spiffe://iotedge/mqttbroker")

	svid, err := workloadapi.FetchJWTSVID(
		ctx,
		jwtsvid.Params{
			Audience: serverID.String(),
		},
		workloadapi.WithAddr("unix://"+m.metadata.spiffeSocketPath),
	)

	if err != nil {
		panic(err)
	}

	m.svid = svid
	m.logger.Debug("mqtte4k got a SPIFFE svid for: ", m.metadata.clientID)
}

// Init parses metadata and creates a new Pub Sub client.
func (m *mqttPubSub) Init(metadata pubsub.Metadata) error {
	mqttMeta, err := parseMQTTMetaData(metadata)
	if err != nil {
		return err
	}
	m.metadata = mqttMeta

	initSpiffeWorkloadApi(m)

	p, err := m.connect()
	if err != nil {
		return err
	}

	m.ctx, m.cancel = context.WithCancel(context.Background())

	// TODO: Make the backoff configurable for constant or exponential
	b := backoff.NewConstantBackOff(5 * time.Second)
	m.backOff = backoff.WithContext(b, m.ctx)

	m.client = p
	m.topics = make(map[string]byte)
	// mqtt broker allows only one connection at a given time from a clientID.
	m.logger.Debug("mqtte4k Init completed for : ", m.metadata.clientID)
	return nil
}

// Publish the topic to mqtt pub sub.
func (m *mqttPubSub) Publish(req *pubsub.PublishRequest) error {
	m.logger.Debugf("mqtte4k publishing topic %s with data: %v", req.Topic, req.Data)

	token := m.client.Publish(req.Topic, m.metadata.qos, m.metadata.retain, req.Data)
	if !token.WaitTimeout(defaultWait) || token.Error() != nil {
		return fmt.Errorf("mqtte4k error from publish: %v", token.Error())
	}

	return nil
}

// Subscribe to the mqtt pub sub topic.
func (m *mqttPubSub) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	m.logger.Debugf("mqtte4k Subscribe request for topic: %s, for Consumer: %s", req.Topic, m.metadata.clientID)
	m.topics[req.Topic] = m.metadata.qos

	go func() {
		token := m.client.SubscribeMultiple(
			m.topics,
			func(client mqtt.Client, mqttMsg mqtt.Message) {
				mqttMsg.AutoAckOff()
				msg := pubsub.NewMessage{
					Topic: mqttMsg.Topic(),
					Data:  mqttMsg.Payload(),
				}

				b := m.backOff
				if m.metadata.backOffMaxRetries >= 0 {
					b = backoff.WithMaxRetries(m.backOff, uint64(m.metadata.backOffMaxRetries))
				}
				if err := retry.NotifyRecover(func() error {
					m.logger.Debugf("Processing MQTTE4K message %s/%d", mqttMsg.Topic(), mqttMsg.MessageID())
					if err := handler(m.ctx, &msg); err != nil {
						return err
					}

					mqttMsg.Ack()

					return nil
				}, b, func(err error, d time.Duration) {
					m.logger.Errorf("Error processing MQTTE4K message: %s/%d. Retrying...", mqttMsg.Topic(), mqttMsg.MessageID())
				}, func() {
					m.logger.Infof("Successfully processed MQTTE4K message after it previously failed: %s/%d", mqttMsg.Topic(), mqttMsg.MessageID())
				}); err != nil {
					m.logger.Errorf("Failed processing MQTTE4K message: %s/%d: %v", mqttMsg.Topic(), mqttMsg.MessageID(), err)
				}
			},
		)
		if err := token.Error(); err != nil {
			m.logger.Errorf("mqtte4k error from subscribe: %v", err)
		}
	}()

	return nil
}

func (m *mqttPubSub) connect() (mqtt.Client, error) {
	uri, err := url.Parse(m.metadata.url)
	if err != nil {
		return nil, err
	}
	opts := m.createClientOptions(uri)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(defaultWait) {
	}
	if err := token.Error(); err != nil {
		return nil, err
	}

	return client, nil
}

func (m *mqttPubSub) createClientOptions(uri *url.URL) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.SetClientID(m.svid.ID.String())
	opts.SetCleanSession(m.metadata.cleanSession)
	opts.AddBroker(uri.String())
	opts.SetKeepAlive(time.Duration(m.metadata.keepAliveDuration) * time.Second)
	opts.SetUsername(m.svid.ID.String())
	opts.SetPassword(m.svid.Marshal())
	return opts
}

func (m *mqttPubSub) Close() error {
	m.cancel()

	if m.client != nil {
		m.client.Disconnect(100)
	}

	return nil
}

func (m *mqttPubSub) Features() []pubsub.Feature {
	return nil
}

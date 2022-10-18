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
	"os"
	"context"
	"errors"
	"net"
	"time"
    "crypto/md5"
    "encoding/hex"

	"github.com/cenkalti/backoff/v4"
	mqtt "github.com/eclipse/paho.golang/paho"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/jwtsvid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
)

const (
	// errors.
	errorMsgPrefix = "mqtte4k pub sub error:"
)

// mqttPubSub type allows sending and receiving data to/from MQTT broker.
type mqttPubSub struct {
	client   *mqtt.Client
	metadata *metadata
	logger   logger.Logger
	topics   map[string]byte

	ctx     context.Context
	cancel  context.CancelFunc
	backOff backoff.BackOff
	svid    *jwtsvid.SVID
}

// NewMQTTPubSub returns a new mqttPubSub instance.
func NewMQTTE4KPubSub(logger logger.Logger) pubsub.PubSub {
	return &mqttPubSub{
		logger:          logger,
	}
}

func initSpiffeWorkloadApi(m *mqttPubSub) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	serverID := spiffeid.RequireFromString(m.metadata.spiffeBrokerAudience)

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
	m.logger.Debugf("mqtte4k got a SPIFFE SVID id: %s", svid.ID.String())
}

// Init parses metadata and creates a new Pub Sub client.
func (m *mqttPubSub) Init(metadata pubsub.Metadata) error {
	mqttMeta, err := parseMQTTMetaData(metadata, m.logger)
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
	if req.Topic == "" {
		return errors.New("topic name is empty")
	}

	// Note this can contain PII
	// m.logger.Debugf("mqtt publishing topic %s with data: %v", req.Topic, req.Data)
	m.logger.Debugf("mqtt publishing topic %s", req.Topic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if puback, err := m.client.Publish(ctx, &mqtt.Publish {
		Topic: req.Topic,
		QoS: m.metadata.qos,
		Retain: m.metadata.retain,
		Payload: []byte(req.Data),
	}); err != nil {
		m.logger.Debugf("mqtte4k error sending message on topic %s with data: %v", req.Topic, req.Data)
		if puback != nil {
			m.logger.Debugf("mqtte4k error PubAck - Reason: %d", puback.ReasonCode)
		}
		m.logger.Debugf("Error: %s", err.Error())
		return err
	}

	return nil
}

// Subscribe to the mqtt pub sub topic.
func (m *mqttPubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if ctxErr := m.ctx.Err(); ctxErr != nil {
		// If the global context has been canceled, we do not allow more subscriptions
		return ctxErr
	}

	if req.Topic == "" {
		return errors.New("topic name is empty")
	}

	m.logger.Debugf("mqtte4k Subscribe request for topic: %s, for Consumer: %s", req.Topic, m.metadata.clientID)

	if m.client.Router == nil {
		m.client.Router = mqtt.NewStandardRouter()
	}

	m.client.Router.RegisterHandler(req.Topic, func(mqttMsg *mqtt.Publish) {
		msg := pubsub.NewMessage{
			Topic: mqttMsg.Topic,
			Data:  mqttMsg.Payload,
		}

		b := m.backOff
		if m.metadata.backOffMaxRetries >= 0 {
			b = backoff.WithMaxRetries(m.backOff, uint64(m.metadata.backOffMaxRetries))
		}
		if err := retry.NotifyRecover(func() error {
			m.logger.Debugf("mqtte4k Processing MQTTE4K message %s/%d", mqttMsg.Topic, mqttMsg.PacketID)
			if err := handler(m.ctx, &msg); err != nil {
				return err
			}
			m.client.Ack(mqttMsg)
			return nil
		}, b, func(err error, d time.Duration) {
			m.logger.Errorf("mqtte4k Error processing MQTTE4K message: %s/%d. Retrying...", mqttMsg.Topic, mqttMsg.PacketID)
		}, func() {
			m.logger.Debugf("mqtte4k Successfully processed MQTTE4K message after it previously failed: %s/%d", mqttMsg.Topic, mqttMsg.PacketID)
		}); err != nil {
			m.logger.Errorf("mqtte4k Failed processing MQTTE4K message: %s/%d: %v", mqttMsg.Topic, mqttMsg.PacketID, err)
		}})

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	suback, err := m.client.Subscribe(ctx, &mqtt.Subscribe{
		Subscriptions: map[string]mqtt.SubscribeOptions{
			req.Topic: {QoS: m.metadata.qos},
		},
	})

	if err != nil {
		m.logger.Debugf("mqtte4k failed to subscribe to topic %s with qos: %v", req.Topic, m.metadata.qos)
		m.logger.Debugf("mqtte4k failed to subscribe ERROR: %s", err.Error())
		if(suback != nil) {
			m.logger.Debugf("mqtte4k SUBACK: ReasonCode:%v Properties:\n%s", suback.Reasons,suback.Properties)
		}
		m.logger.Errorf("mqtte4k Error: %s", err.Error())
	}

	return nil
}

func (m *mqttPubSub) connect() (*mqtt.Client, error) {
	conn, err := net.Dial("tcp", m.metadata.url)
	if err != nil {
		m.logger.Debugf("mqtte4k Failed to connect to tcp://%s", m.metadata.url)
		return nil, err
	}

	c := mqtt.NewClient(mqtt.ClientConfig{
		Conn: conn,
		EnableManualAcknowledgment : false,
	})

	cp := m.createClientOptions()
	cp.UsernameFlag = true
	cp.PasswordFlag = true

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ca, err := c.Connect(ctx, cp)
	if err != nil {
		m.logger.Debugf(err.Error())
		return nil, err
	}

	if ca.ReasonCode != 0 {
		m.logger.Debugf("mqtte4k Failed to connect to %s : %d - %s", m.metadata.url, ca.ReasonCode, ca.Properties.ReasonString)
		return nil, err
	}

	return c, nil
}

func (m *mqttPubSub) createClientOptions() *mqtt.Connect {
	cp := &mqtt.Connect{
		KeepAlive:  m.metadata.keepAliveDuration,
		ClientID:   getMD5HashClientID(m.metadata.clientID ,m.svid.ID.String()),
		CleanStart: m.metadata.cleanSession,
		Username:   m.svid.ID.String(),
		Password:   []byte(m.svid.Marshal()),
	}
	return cp
}

func (m *mqttPubSub) Close() error {
	m.cancel()

	if m.client != nil {
		d := &mqtt.Disconnect{ReasonCode: 0}
		m.client.Disconnect(d)
	}

	return nil
}

func (m *mqttPubSub) Features() []pubsub.Feature {
	return nil
}

func getMD5HashClientID(clientID string, svidID string) string {
	text := svidID + os.Getenv("POD_NAME")
	hash := md5.Sum([]byte(text))

	hexString := ""
	if len(clientID) > 12 {
		hexString = clientID[:12] + "-" + hex.EncodeToString(hash[:])
	} else {
		hexString = clientID + "-" + hex.EncodeToString(hash[:])
	}

	return hexString[:23]
}
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

package e4k

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"

	"github.com/cenkalti/backoff/v4"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"

	metadata "github.com/dapr/components-contrib/metadata"
	mqtt "github.com/eclipse/paho.golang/paho"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/jwtsvid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
)

const (
	// Keys.
	mqttURL                 = "url"
	mqttQOS                 = "mqttqos"
	mqttRetain              = "retain"
	mqttClientIdPrefix      = "clientIdPrefix"
	mqttCleanSession        = "cleanSession"
	mqttBackOffMaxRetries   = "backOffMaxRetries"
	mqttKeepAliveDuration   = "keepAlive"
	mqttResponseTopicPrefix = "responseTopicPrefix"

	// Defaults.
	defaultQOS                     = 1
	defaultRetain                  = false
	defaultWait                    = 3 * time.Second
	defaultCleanSession            = true
	defaultKeepAliveDuration       = 30
	defaultMqttResponseTopicPrefix = "response_topic"

	// Auth keys.
	brokerAuthMethod     = "brokerAuthMethod"
	satTokenPath         = "satTokenPath"
	spiffeSocketPath     = "spiffeSocketPath"
	spiffeBrokerAudience = "spiffeBrokerAudience"

	// defaultClientID prefix
	clientIdPrefix = "e4kd-"

	// errors.
	errorMsgPrefix      = "e4k statestore error:"
	mqttResponseTimeout = 200
)

// StateStore Type.
type StateStore struct {
	state.DefaultBulkStore
	client           *mqtt.Client
	metadata         *e4kMetadata
	cancel           context.CancelFunc
	backOff          backoff.BackOff
	svid             *jwtsvid.SVID
	satToken         string
	ctx              context.Context
	correlationMap   map[string]bool
	responsesChannel chan *mqtt.Publish

	features []state.Feature
	logger   logger.Logger
}

type e4kMetadata struct {
	url                  string
	clientIdPrefix       string
	qos                  byte
	retain               bool
	cleanSession         bool
	backOffMaxRetries    int
	keepAliveDuration    uint16
	brokerAuthMethod     string
	satTokenPath         string
	spiffeSocketPath     string
	spiffeBrokerAudience string
	responseTopic        string
}

func NewAzureE4KStore(logger logger.Logger) state.Store {
	s := &StateStore{
		features: []state.Feature{},
		logger:   logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

func (r *StateStore) GetComponentMetadata() map[string]string {
	metadataStruct := e4kMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}

func populateSATPassword(r *StateStore) {
	token, err := os.ReadFile(r.metadata.satTokenPath)
	if err != nil {
		panic("failed to read SAT from Token file path. Are volume-mount annotations provided?")
	}

	satToken := string(token) // convert token to a String

	r.satToken = satToken
	r.logger.Debugf("mqtte4k got SAT Token")
}

func initSpiffeWorkloadApi(r *StateStore) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	serverID := spiffeid.RequireFromString(r.metadata.spiffeBrokerAudience)

	svid, err := workloadapi.FetchJWTSVID(
		ctx,
		jwtsvid.Params{
			Audience: serverID.String(),
		},
		workloadapi.WithAddr("unix://"+r.metadata.spiffeSocketPath),
	)

	if err != nil {
		panic(err)
	}

	r.svid = svid
	r.logger.Debugf("e4k state store got a SPIFFE SVID id: %s", svid.ID.String())
}

func (r *StateStore) connect() (*mqtt.Client, error) {
	conn, err := net.Dial("tcp", r.metadata.url)
	if err != nil {
		r.logger.Debugf("e4k state store Failed to connect to tcp://%s", r.metadata.url)
		return nil, err
	}

	c := mqtt.NewClient(mqtt.ClientConfig{
		Conn:                       conn,
		EnableManualAcknowledgment: false,
	})

	cp := r.createClientOptions()
	cp.UsernameFlag = true
	cp.PasswordFlag = true

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ca, err := c.Connect(ctx, cp)
	if err != nil {
		r.logger.Debugf(err.Error())
		return nil, err
	}

	if ca.ReasonCode != 0 {
		r.logger.Debugf("e4k state store Failed to connect to %s : %d - %s", r.metadata.url, ca.ReasonCode, ca.Properties.ReasonString)
		return nil, err
	}

	return c, nil
}

func (r *StateStore) createClientOptions() *mqtt.Connect {
	var username string
	var password []byte
	if r.metadata.brokerAuthMethod == "SAT" {
		username = "$sat"
		password = []byte(r.satToken)
	} else {
		username = r.svid.ID.String()
		password = []byte(r.svid.Marshal())
	}

	cp := &mqtt.Connect{
		KeepAlive:  r.metadata.keepAliveDuration,
		ClientID:   getMD5HashClientID(r.metadata.clientIdPrefix),
		CleanStart: r.metadata.cleanSession,
		Username:   username,
		Password:   password,
	}
	return cp
}

// Subscribe to the mqtt pub sub topic.
func (r *StateStore) subscribeResponseTopic() error {
	r.logger.Debugf("e4k state store subscribing to Response Topic: %s", r.metadata.responseTopic)

	default_err := errors.New("e4k state store error while subscribing to response topic")

	r.client.Router = mqtt.NewSingleHandlerRouter(func(mqttMsg *mqtt.Publish) {
		// r.responses[mqttMsg.Topic] = mqttMsg.Payload
		correlationString := string(mqttMsg.Properties.CorrelationData[:])
		if _, ok := r.correlationMap[correlationString]; ok {
			r.responsesChannel <- mqttMsg
			r.logger.Debugf("e4k state store Successfully processed MQTT message: %s/%d", mqttMsg.Topic, mqttMsg.PacketID)
			delete(r.correlationMap, correlationString)
		} else {
			r.logger.Warnf("e4k state store received unexpected or older MQTT message: %s/%d", mqttMsg.Topic, mqttMsg.PacketID)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	suback, err := r.client.Subscribe(ctx, &mqtt.Subscribe{
		Subscriptions: map[string]mqtt.SubscribeOptions{
			r.metadata.responseTopic: {QoS: r.metadata.qos},
		},
	})

	if err != nil {
		r.logger.Debugf("e4k state store failed to subscribe to response topic %s with qos: %v", r.metadata.responseTopic, r.metadata.qos)
		r.logger.Debugf("e4k state store failed to subscribe ERROR: %s", err.Error())

		if suback != nil {
			r.logger.Debugf("e4k state store SUBACK: ReasonCode:%v Properties:\n%s", suback.Reasons, suback.Properties)
		}
		r.logger.Errorf("e4k state store Error: %s", err.Error())
		return err
	}

	default_err = err
	r.logger.Debugf("e4k state store successfully subscribed to response topic")
	return default_err
}

// Init the connection to blob storage, optionally creates a blob container if it doesn't exist.
func (r *StateStore) Init(metadata state.Metadata) error {
	meta, err := getE4KStorageMetadata(metadata)
	if err != nil {
		return err
	}

	r.metadata = meta

	if r.metadata.brokerAuthMethod == "SAT" {
		populateSATPassword(r)
	} else {
		initSpiffeWorkloadApi(r)
	}

	p, err := r.connect()
	if err != nil {
		return err
	}

	r.client = p
	r.ctx, r.cancel = context.WithCancel(context.Background())

	err = r.subscribeResponseTopic()
	if err != nil {
		return err
	}

	r.responsesChannel = make(chan *mqtt.Publish)
	r.correlationMap = make(map[string]bool)
	r.logger.Debugf("e4k state store Init Finished")

	return nil
}

// Features returns the features available in this state store.
func (r *StateStore) Features() []state.Feature {
	return nil
}

// Delete the state.
func (r *StateStore) Delete(req *state.DeleteRequest) error {
	r.logger.Debugf("e4k state store Delete: %s", req.Key)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub_topic := fmt.Sprintf("$store/%s", req.Key)

	r.logger.Debugf("e4k state store Publishing topic %s", pub_topic)

	props := &mqtt.PublishProperties{}
	props.User.Add("OPERATION", "DELETE")

	props.ResponseTopic = r.metadata.responseTopic
	correlation_string := uuid.New().String()
	r.correlationMap[correlation_string] = true
	props.CorrelationData = []byte(correlation_string)

	if puback, err := r.client.Publish(ctx, &mqtt.Publish{
		Topic:      pub_topic,
		QoS:        r.metadata.qos,
		Payload:    make([]byte, 0),
		Properties: props,
	}); err != nil {
		r.logger.Debugf("e4k state store error sending message on topic %s", pub_topic)
		if puback != nil {
			r.logger.Debugf("mqtte4k error PubAck - Reason: %d", puback.ReasonCode)
		}
		r.logger.Debugf("Error: %s", err.Error())
		return err
	}

	// Waiting for Request-Response to come back
	select {
	case res := <-r.responsesChannel:
		if res.Properties.User.Get("STATUS") == "error" {
			if res.Properties.User.Get("MESSAGE") == "Key not found" {
				r.logger.Debugf("e4k state store Key: %s not found", req.Key)
				return nil
			} else {
				error_log := fmt.Sprintf("e4k state store Error in Deleting Key: %s, Message: %s", req.Key, res.Properties.User.Get("MESSAGE"))
				r.logger.Debugf(error_log)
				err := errors.New(error_log)
				return err
			}
		}
	case <-time.After(mqttResponseTimeout * time.Second):
		error_log := fmt.Sprintf("e4k state store Delete unsuccessful for: %s, Key might be not present or E4K broker is slow", req.Key)
		r.logger.Debugf(error_log)
		err := errors.New(error_log)
		return err
	}

	r.logger.Debugf("e4k state store Delete successful for: %s", req.Key)

	return nil
}

// Get the state.
func (r *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	r.logger.Debugf("e4k state store Get: %s", req.Key)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub_topic := fmt.Sprintf("$store/%s", req.Key)

	r.logger.Debugf("e4k state store Publishing topic %s", pub_topic)

	props := &mqtt.PublishProperties{}
	props.User.Add("OPERATION", "GET")

	props.ResponseTopic = r.metadata.responseTopic
	correlation_string := uuid.New().String()
	r.correlationMap[correlation_string] = true
	props.CorrelationData = []byte(correlation_string)

	if puback, err := r.client.Publish(ctx, &mqtt.Publish{
		Topic:      pub_topic,
		QoS:        r.metadata.qos,
		Payload:    make([]byte, 0),
		Properties: props,
	}); err != nil {
		r.logger.Debugf("e4k state store error sending message on topic %s", pub_topic)
		if puback != nil {
			r.logger.Debugf("mqtte4k error PubAck - Reason: %d", puback.ReasonCode)
		}
		r.logger.Debugf("Error: %s", err.Error())
		return nil, err
	}

	// Waiting for Request-Response to come back
	select {
	case res := <-r.responsesChannel:
		if res.Properties.User.Get("STATUS") == "error" {
			if res.Properties.User.Get("MESSAGE") == "Key not found" {
				r.logger.Debugf("e4k state store Get Key: %s not found", req.Key)
			} else {
				error_log := fmt.Sprintf("e4k state store Error in Getting Key: %s, Message: %s", req.Key, res.Properties.User.Get("MESSAGE"))
				r.logger.Debugf(error_log)
				err := errors.New(error_log)
				return nil, err
			}
		} else {
			r.logger.Debugf("e4k state store Get successful for: %s", req.Key)

			return &state.GetResponse{
				Data:        res.Payload,
				ETag:        nil,
				ContentType: &res.Properties.ContentType,
			}, nil
		}
	case <-time.After(mqttResponseTimeout * time.Second):
		error_log := fmt.Sprintf("e4k state store Get unsuccessful for: %s, Key might be not present or E4K broker is slow", req.Key)
		r.logger.Debugf(error_log)
		err := errors.New(error_log)
		return nil, err
	}

	return &state.GetResponse{}, nil
}

func (r *StateStore) marshal(req *state.SetRequest) []byte {
	var v string
	b, ok := req.Value.([]byte)
	if ok {
		v = string(b)
	} else {
		v, _ = jsoniter.MarshalToString(req.Value)
	}

	return []byte(v)
}

// Set the state.
func (r *StateStore) Set(req *state.SetRequest) error {
	r.logger.Debugf("e4k state store Set %s", req.Key)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub_topic := fmt.Sprintf("$store/%s", req.Key)

	r.logger.Debugf("e4k state store Publishing topic %s", pub_topic)

	props := &mqtt.PublishProperties{}
	props.User.Add("OPERATION", "UPSERT")

	props.ResponseTopic = r.metadata.responseTopic
	correlation_string := uuid.New().String()
	r.correlationMap[correlation_string] = true
	props.CorrelationData = []byte(correlation_string)

	if puback, err := r.client.Publish(ctx, &mqtt.Publish{
		Topic:      pub_topic,
		QoS:        r.metadata.qos,
		Payload:    r.marshal(req),
		Properties: props,
	}); err != nil {
		r.logger.Debugf("e4k state store error sending message on topic %s", pub_topic)
		if puback != nil {
			r.logger.Debugf("mqtte4k error PubAck - Reason: %d", puback.ReasonCode)
		}
		r.logger.Debugf("Error: %s", err.Error())
		return err
	}

	// Waiting for Request-Response to come back
	select {
	case res := <-r.responsesChannel:
		if res.Properties.User.Get("STATUS") == "error" {
			error_log := fmt.Sprintf("e4k state store Error in Setting Key: %s, Message: %s", req.Key, res.Properties.User.Get("MESSAGE"))
			r.logger.Debugf(error_log)
			err := errors.New(error_log)
			return err
		}
	case <-time.After(mqttResponseTimeout * time.Second):
		error_log := fmt.Sprintf("e4k state store Set Key unsuccessful for: %s, internal error or E4K broker is slow", req.Key)
		r.logger.Debugf(error_log)
		err := errors.New(error_log)
		return err
	}

	r.logger.Debugf("e4k state store Set successful for: %s", req.Key)

	return nil
}

func getE4KStorageMetadata(md state.Metadata) (*e4kMetadata, error) {
	m := e4kMetadata{}

	// required configuration settings
	if val, ok := md.Properties[mqttURL]; ok && val != "" {
		m.url = val
	} else {
		return &m, fmt.Errorf("%s missing url", errorMsgPrefix)
	}

	if val, ok := md.Properties[brokerAuthMethod]; ok && val == "spiffe" {
		m.brokerAuthMethod = "spiffe"
		if val, ok := md.Properties[spiffeSocketPath]; ok && val != "" {
			m.spiffeSocketPath = val

		} else {
			return &m, fmt.Errorf("%s Invalid or Missing spiffeSocketPath", errorMsgPrefix)
		}

		if val, ok := md.Properties[spiffeBrokerAudience]; ok && val != "" {
			m.spiffeBrokerAudience = val
		} else {
			return &m, fmt.Errorf("%s Invalid or Missing spiffeBrokerAudience", errorMsgPrefix)
		}
	} else {
		m.brokerAuthMethod = "SAT"
		if val, ok := md.Properties[satTokenPath]; ok && val != "" {
			m.satTokenPath = val

		} else {
			return &m, fmt.Errorf("%s Invalid or Missing satTokenPath", errorMsgPrefix)
		}
	}

	// optional configuration settings

	if val, ok := md.Properties[mqttClientIdPrefix]; ok && val != "" {
		m.clientIdPrefix = val
	} else {
		m.clientIdPrefix = clientIdPrefix + uuid.New().String()
	}

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
		m.keepAliveDuration = uint16(keepAliveDurationInt)
	}

	topic_suffix := defaultMqttResponseTopicPrefix
	if val, ok := md.Properties[mqttResponseTopicPrefix]; ok && val != "" {
		topic_suffix = val
	}
	pod_name := os.Getenv("POD_NAME")
	responseTopic := pod_name + "/" + topic_suffix
	m.responseTopic = strings.ToLower(responseTopic)

	return &m, nil
}

func getMD5HashClientID(clientID string) string {
	text := os.Getenv("POD_NAME")
	hash := md5.Sum([]byte(text))

	hexString := ""
	if len(clientID) > 12 {
		hexString = clientID[:12] + "-" + hex.EncodeToString(hash[:])
	} else {
		hexString = clientID + "-" + hex.EncodeToString(hash[:])
	}

	return hexString[:23]
}

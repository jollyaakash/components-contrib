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
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type metadata struct {
	url               string
	clientID          string
	qos               byte
	retain            bool
	cleanSession      bool
	backOffMaxRetries int
	keepAliveDuration uint16
	spiffeSocketPath  string
	spiffeBrokerAudience string
}

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

	// Defaults.
	defaultQOS               = 1
	defaultRetain            = false
	defaultWait              = 3 * time.Second
	defaultCleanSession      = true
	defaultKeepAliveDuration = 30
	defaultSpiffeSocketPath  = "/run/azedge/sockets/workloadapi.sock"
	defaultSpiffeBrokerAudience  = "spiffe://iotedge/mqttbroker"


	// Spiffe keys.
	spiffeSocketPath = "spiffeSocketPath"
	spiffeBrokerAudience = "spiffeBrokerAudience"
)

func parseMQTTMetaData(md pubsub.Metadata, log logger.Logger) (*metadata, error) {
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
		m.keepAliveDuration = uint16(keepAliveDurationInt)
	}

	m.spiffeSocketPath = defaultSpiffeSocketPath
	if val, ok := md.Properties[spiffeSocketPath]; ok && val != "" {
		m.spiffeSocketPath = val
	} else {
		return &m, fmt.Errorf("%s Invalid or Missing spiffeSocketPath", errorMsgPrefix)
	}

	m.spiffeBrokerAudience = defaultSpiffeBrokerAudience
	if val, ok := md.Properties[spiffeBrokerAudience]; ok && val != "" {
		m.spiffeBrokerAudience = val
	} else {
		return &m, fmt.Errorf("%s Invalid or Missing spiffeBrokerAudience", errorMsgPrefix)
	}

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
			return &m, fmt.Errorf("%s invalid cleanSession %s, %s", errorMsgPrefix, val, err)
		}
	}

	return &m, nil
}
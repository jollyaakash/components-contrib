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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func getFakeProperties() map[string]string {
	return map[string]string{
		"consumerID":         "client",
		mqttURL:              "tcp://fakeUser:fakePassword@fake.mqtt.host:1883",
		mqttQOS:              "1",
		mqttRetain:           "true",
		mqttCleanSession:     "false",
		brokerAuthMethod:     "spiffe",
		spiffeSocketPath:     "spiffeSocketPath",
		spiffeBrokerAudience: "spiffeBrokerAudience",
	}
}

func TestParseMetadata(t *testing.T) {
	log := logger.NewLogger("test")
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}

		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[mqttURL], m.url)
		assert.Equal(t, byte(1), m.qos)
		assert.Equal(t, true, m.retain)
		assert.Equal(t, false, m.cleanSession)
	})

	t.Run("url is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[mqttURL] = ""

		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.EqualError(t, err, errors.New("mqtte4k pub sub error: missing url").Error())
		assert.Equal(t, fakeProperties[mqttURL], m.url)
	})

	t.Run("qos and retain is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[mqttQOS] = ""
		fakeMetaData.Properties[mqttRetain] = ""

		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[mqttURL], m.url)
		assert.Equal(t, byte(1), m.qos)
		assert.Equal(t, false, m.retain)
	})

	t.Run("invalid clean session field", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[mqttCleanSession] = "randomString"

		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.Contains(t, err.Error(), "invalid clean session")
		assert.Equal(t, fakeProperties[mqttURL], m.url)
	})

}

package amqp

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

var helperTo = "ActiveMQ.DLQ"

var exampleEncodedMessages = []struct {
	label    string
	expected Message
	encoded  []byte
}{
	{
		label: "SwiftMQ message",
		expected: Message{
			Format: 0,
			Header: &MessageHeader{
				Durable: true, Priority: 4,
				TTL: 0, FirstAcquirer: false,
				DeliveryCount: 0,
			},
			Properties: &MessageProperties{
				MessageID: "7735812932138480283/1/12",
				To:        &helperTo,
			},
			ApplicationProperties: map[string]interface{}{
				"prop002":       "v2",
				"prop000000003": int64(100000),
				"prop4":         "val000004",
				"prop01":        "val001",
			},
			Value: `{"id":"000000000","prop4":"val000004","prop002Code":"v2","___prop000000003":10.0,"_______prop000000003":"10.0","prop0005":100,"_________prop01":"val001"}`,
		},
		encoded: []byte{
			0, 128, 0, 0, 0, 0, 0, 0, 0, 112, 192, 7, 5, 65, 80, 4, 64, 66, 67,
			0, 128, 0, 0, 0, 0, 0, 0, 0, 115, 192, 42, 3, 161, 24, 55, 55, 51, 53, 56, 49, 50, 57, 51, 50, 49, 51, 56, 52, 56, 48, 50, 56, 51, 47, 49, 47, 49, 50, 64, 161, 12, 65, 99, 116, 105, 118, 101, 77, 81, 46, 68, 76, 81,
			0, 128, 0, 0, 0, 0, 0, 0, 0, 116, 193, 72, 8, 161, 6, 112, 114, 111, 112, 48, 49, 161, 6, 118, 97, 108, 48, 48, 49, 161, 7, 112, 114, 111, 112, 48, 48, 50, 161, 2, 118, 50, 161, 13, 112, 114, 111, 112, 48, 48, 48, 48, 48, 48, 48, 48, 51, 129, 0, 0, 0, 0, 0, 1, 134, 160, 161, 5, 112, 114, 111, 112, 52, 161, 9, 118, 97, 108, 48, 48, 48, 48, 48, 52,
			0, 128, 0, 0, 0, 0, 0, 0, 0, 119, 161, 153, 123, 34, 105, 100, 34, 58, 34, 48, 48, 48, 48, 48, 48, 48, 48, 48, 34, 44, 34, 112, 114, 111, 112, 52, 34, 58, 34, 118, 97, 108, 48, 48, 48, 48, 48, 52, 34, 44, 34, 112, 114, 111, 112, 48, 48, 50, 67, 111, 100, 101, 34, 58, 34, 118, 50, 34, 44, 34, 95, 95, 95, 112, 114, 111, 112, 48, 48, 48, 48, 48, 48, 48, 48, 51, 34, 58, 49, 48, 46, 48, 44, 34, 95, 95, 95, 95, 95, 95, 95, 112, 114, 111, 112, 48, 48, 48, 48, 48, 48, 48, 48, 51, 34, 58, 34, 49, 48, 46, 48, 34, 44, 34, 112, 114, 111, 112, 48, 48, 48, 53, 34, 58, 49, 48, 48, 44, 34, 95, 95, 95, 95, 95, 95, 95, 95, 95, 112, 114, 111, 112, 48, 49, 34, 58, 34, 118, 97, 108, 48, 48, 49, 34, 125,
		},
	},
}

func TestMessageUnmarshaling(t *testing.T) {
	for _, tt := range exampleEncodedMessages {
		t.Run(tt.label, func(t *testing.T) {
			var msg Message

			if err := msg.UnmarshalBinary(tt.encoded); err != nil {
				t.Fatal("failed to decode message: ", err)
			}

			if diff := cmp.Diff(tt.expected, msg, cmpopts.IgnoreUnexported(Message{})); diff != "" {
				t.Fatalf("decoded message differs from expected (-expected +got): %s", diff)
			}
		})
	}
}

func TestMessageWithSequence(t *testing.T) {
	m := &Message{
		Sequence: [][]interface{}{
			{"hello1", "world1", 11, 12, 13},
			{"hello2", "world2", 21, 22, 23},
		},
	}

	bytes, err := m.MarshalBinary()
	require.NoError(t, err)

	newM := &Message{}
	err = newM.UnmarshalBinary(bytes)
	require.NoError(t, err)

	require.EqualValues(t, [][]interface{}{
		{"hello1", "world1", int64(11), int64(12), int64(13)},
		{"hello2", "world2", int64(21), int64(22), int64(23)},
	}, newM.Sequence)
}

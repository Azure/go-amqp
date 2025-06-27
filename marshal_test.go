package amqp

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/buffer"
	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/test"
)

var exampleFrames = []struct {
	label string
	frame frames.Frame
}{
	{
		label: "transfer",
		frame: frames.Frame{
			Type:    frames.TypeAMQP,
			Channel: 10,
			Body: &frames.PerformTransfer{
				Handle:             34983,
				DeliveryID:         uint32Ptr(564),
				DeliveryTag:        []byte("foo tag"),
				MessageFormat:      uint32Ptr(34),
				Settled:            true,
				More:               true,
				ReceiverSettleMode: rcvSettle(ReceiverSettleModeSecond),
				State:              &encoding.StateReceived{},
				Resume:             true,
				Aborted:            true,
				Batchable:          true,
				Payload:            []byte("very important payload"),
			},
		},
	},
}

func TestFrameMarshalUnmarshal(t *testing.T) {
	for _, tt := range exampleFrames {
		t.Run(tt.label, func(t *testing.T) {
			var buf buffer.Buffer

			err := frames.Write(&buf, tt.frame)
			if err != nil {
				t.Fatalf("%+v", err)
			}

			header, err := frames.ParseHeader(&buf)
			if err != nil {
				t.Fatalf("%+v", err)
			}

			want := tt.frame
			if header.Channel != want.Channel {
				t.Errorf("Expected channel to be %d, but it is %d", want.Channel, header.Channel)
			}
			if header.FrameType != uint8(want.Type) {
				t.Errorf("Expected channel to be %d, but it is %d", want.Type, header.FrameType)
			}

			payload, err := frames.ParseBody(&buf)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			if !test.Equal(want.Body, payload) {
				t.Errorf("Roundtrip produced different results:\n %s", test.Diff(want.Body, payload))
			}
		})
	}
}

func BenchmarkFrameMarshal(b *testing.B) {
	for _, tt := range exampleFrames {
		b.Run(tt.label, func(b *testing.B) {
			b.ReportAllocs()
			var buf buffer.Buffer

			for i := 0; i < b.N; i++ {
				err := frames.Write(&buf, tt.frame)
				if err != nil {
					b.Errorf("%+v", err)
				}
				bytesSink = buf.Bytes()
				buf.Reset()
			}
		})
	}
}
func BenchmarkFrameUnmarshal(b *testing.B) {
	for _, tt := range exampleFrames {
		b.Run(tt.label, func(b *testing.B) {
			b.ReportAllocs()
			var buf buffer.Buffer
			err := frames.Write(&buf, tt.frame)
			if err != nil {
				b.Errorf("%+v", err)
			}
			data := buf.Bytes()
			buf.Reset()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				buf := buffer.New(data)
				_, err := frames.ParseHeader(buf)
				if err != nil {
					b.Errorf("%+v", err)
				}

				_, err = frames.ParseBody(buf)
				if err != nil {
					b.Errorf("%+v", err)
				}
			}
		})
	}
}

var bytesSink []byte

func BenchmarkMarshal(b *testing.B) {
	for _, typ := range allTypes {
		b.Run(fmt.Sprintf("%T", typ), func(b *testing.B) {
			b.ReportAllocs()
			var buf buffer.Buffer

			for i := 0; i < b.N; i++ {
				err := encoding.Marshal(&buf, typ)
				if err != nil {
					b.Errorf("%+v", err)
				}
				bytesSink = buf.Bytes()
				buf.Reset()
			}
		})
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	for _, type_ := range allTypes {
		if type_ == nil {
			continue
		}
		b.Run(fmt.Sprintf("%T", type_), func(b *testing.B) {
			var buf buffer.Buffer
			err := encoding.Marshal(&buf, type_)
			if err != nil {
				b.Errorf("%+v", err)
			}
			data := buf.Bytes()
			newType := reflect.New(reflect.TypeOf(type_)).Interface()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				err = encoding.Unmarshal(buffer.New(data), newType)
				if err != nil {
					b.Errorf("%v", err)
				}
			}
		})
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	_, updateFuzzCorpus := os.LookupEnv("UPDATE_FUZZ_CORPUS")

	for _, type_ := range allTypes {
		t.Run(fmt.Sprintf("%T", type_), func(t *testing.T) {
			var buf buffer.Buffer
			err := encoding.Marshal(&buf, type_)
			if err != nil {
				t.Fatalf("%+v", err)
			}

			if updateFuzzCorpus {
				name := fmt.Sprintf("%T.bin", type_)
				name = strings.TrimPrefix(name, "amqp.")
				name = strings.TrimPrefix(name, "*amqp.")
				path := filepath.Join("fuzz/marshal/corpus", name)
				err = os.WriteFile(path, buf.Bytes(), 0644)
				if err != nil {
					t.Error(err)
				}
			}

			// handle special case around nil type
			if type_ == nil {
				err = encoding.Unmarshal(&buf, nil)
				if err != nil {
					t.Fatalf("%+v", err)
					return
				}
				return
			}

			newType := reflect.New(reflect.TypeOf(type_))
			err = encoding.Unmarshal(&buf, newType.Interface())
			if err != nil {
				t.Fatalf("%+v", err)
				return
			}
			cmpType := reflect.Indirect(newType).Interface()
			if !test.Equal(type_, cmpType) {
				t.Errorf("Roundtrip produced different results:\n %s", test.Diff(type_, cmpType))
			}
		})
	}
}

// Regression test for time calculation bug.
// https://github.com/vcabbage/amqp/issues/173
func TestIssue173(t *testing.T) {
	var buf buffer.Buffer
	// NOTE: Dates after the Unix Epoch don't trigger the bug, only
	// dates that negative Unix time show the problem.
	want := time.Date(1969, 03, 21, 0, 0, 0, 0, time.UTC)
	err := encoding.Marshal(&buf, want)
	if err != nil {
		t.Fatal(err)
	}
	var got time.Time
	err = encoding.Unmarshal(&buf, &got)
	if err != nil {
		t.Fatal(err)
	}
	if d := test.Diff(want, got); d != "" {
		t.Fatal(d)
	}
}

func TestReadAny(t *testing.T) {
	for _, type_ := range generalTypes {
		t.Run(fmt.Sprintf("%T", type_), func(t *testing.T) {
			var buf buffer.Buffer
			err := encoding.Marshal(&buf, type_)
			if err != nil {
				t.Errorf("%+v", err)
			}

			got, err := encoding.ReadAny(&buf)
			if err != nil {
				t.Fatalf("%+v", err)
			}

			if !test.Equal(type_, got) {
				t.Errorf("Roundtrip produced different results:\n %s", test.Diff(type_, got))
			}
		})
	}
}

var (
	allTypes = append(protoTypes, generalTypes...)

	remoteChannel = uint16(4321)

	protoTypes = []any{
		&frames.PerformOpen{
			ContainerID:         "foo",
			Hostname:            "bar.host",
			MaxFrameSize:        4200,
			ChannelMax:          13,
			OutgoingLocales:     []encoding.Symbol{"fooLocale"},
			IncomingLocales:     []encoding.Symbol{"barLocale"},
			OfferedCapabilities: []encoding.Symbol{"fooCap"},
			DesiredCapabilities: []encoding.Symbol{"barCap"},
			Properties: map[encoding.Symbol]any{
				"fooProp": int32(45),
			},
		},
		&frames.PerformBegin{
			RemoteChannel:       &remoteChannel,
			NextOutgoingID:      730000,
			IncomingWindow:      9876654,
			OutgoingWindow:      123555,
			HandleMax:           9757,
			OfferedCapabilities: []encoding.Symbol{"fooCap"},
			DesiredCapabilities: []encoding.Symbol{"barCap"},
			Properties: map[encoding.Symbol]any{
				"fooProp": int32(45),
			},
		},
		&frames.PerformAttach{
			Name:               "fooName",
			Handle:             435982,
			Role:               encoding.RoleSender,
			SenderSettleMode:   sndSettle(SenderSettleModeMixed),
			ReceiverSettleMode: rcvSettle(ReceiverSettleModeSecond),
			Source: &frames.Source{
				Address:      "fooAddr",
				Durable:      DurabilityUnsettledState,
				ExpiryPolicy: ExpiryPolicyLinkDetach,
				Timeout:      635,
				Dynamic:      true,
				DynamicNodeProperties: map[encoding.Symbol]any{
					"lifetime-policy": encoding.DeleteOnClose,
				},
				DistributionMode: "some-mode",
				Filter: encoding.Filter{
					"foo:filter": &encoding.DescribedType{
						Descriptor: "foo:filter",
						Value:      "bar value",
					},
				},
				Outcomes:     []encoding.Symbol{"amqp:accepted:list"},
				Capabilities: []encoding.Symbol{"barCap"},
			},
			Target: &frames.Target{
				Address:      "fooAddr",
				Durable:      DurabilityUnsettledState,
				ExpiryPolicy: ExpiryPolicyLinkDetach,
				Timeout:      635,
				Dynamic:      true,
				DynamicNodeProperties: map[encoding.Symbol]any{
					"lifetime-policy": encoding.DeleteOnClose,
				},
				Capabilities: []encoding.Symbol{"barCap"},
			},
			Unsettled: encoding.Unsettled{
				"fooDeliveryTag": &encoding.StateAccepted{},
			},
			IncompleteUnsettled:  true,
			InitialDeliveryCount: 3184,
			MaxMessageSize:       75983,
			OfferedCapabilities:  []encoding.Symbol{"fooCap"},
			DesiredCapabilities:  []encoding.Symbol{"barCap"},
			Properties: map[encoding.Symbol]any{
				"fooProp": int32(45),
			},
		},
		encoding.Role(true),
		&encoding.Unsettled{
			"fooDeliveryTag": &encoding.StateAccepted{},
		},
		&frames.Source{
			Address:      "fooAddr",
			Durable:      DurabilityUnsettledState,
			ExpiryPolicy: ExpiryPolicyLinkDetach,
			Timeout:      635,
			Dynamic:      true,
			DynamicNodeProperties: map[encoding.Symbol]any{
				"lifetime-policy": encoding.DeleteOnClose,
			},
			DistributionMode: "some-mode",
			Filter: encoding.Filter{
				"foo:filter": &encoding.DescribedType{
					Descriptor: "foo:filter",
					Value:      "bar value",
				},
			},
			Outcomes:     []encoding.Symbol{"amqp:accepted:list"},
			Capabilities: []encoding.Symbol{"barCap"},
		},
		&frames.Target{
			Address:      "fooAddr",
			Durable:      DurabilityUnsettledState,
			ExpiryPolicy: ExpiryPolicyLinkDetach,
			Timeout:      635,
			Dynamic:      true,
			DynamicNodeProperties: map[encoding.Symbol]any{
				"lifetime-policy": encoding.DeleteOnClose,
			},
			Capabilities: []encoding.Symbol{"barCap"},
		},
		&frames.PerformFlow{
			NextIncomingID: uint32Ptr(354),
			IncomingWindow: 4352,
			NextOutgoingID: 85324,
			OutgoingWindow: 24378634,
			Handle:         uint32Ptr(341543),
			DeliveryCount:  uint32Ptr(31341),
			LinkCredit:     uint32Ptr(7634),
			Available:      uint32Ptr(878321),
			Drain:          true,
			Echo:           true,
			Properties: map[encoding.Symbol]any{
				"fooProp": int32(45),
			},
		},
		&frames.PerformTransfer{
			Handle:             34983,
			DeliveryID:         uint32Ptr(564),
			DeliveryTag:        []byte("foo tag"),
			MessageFormat:      uint32Ptr(34),
			Settled:            true,
			More:               true,
			ReceiverSettleMode: rcvSettle(ReceiverSettleModeSecond),
			State:              &encoding.StateReceived{},
			Resume:             true,
			Aborted:            true,
			Batchable:          true,
			Payload:            []byte("very important payload"),
		},
		&frames.PerformDisposition{
			Role:      encoding.RoleSender,
			First:     5644444,
			Last:      uint32Ptr(423),
			Settled:   true,
			State:     &encoding.StateReleased{},
			Batchable: true,
		},
		&frames.PerformDetach{
			Handle: 4352,
			Closed: true,
			Error: &Error{
				Condition:   ErrCondNotAllowed,
				Description: "foo description",
				Info: map[string]any{
					"other": "info",
					"and":   uint16(875),
				},
			},
		},
		&frames.PerformDetach{
			Handle: 4352,
			Closed: true,
			Error: &Error{
				Condition:   ErrCondLinkRedirect,
				Description: "",
				// payload is bigger than map8 encoding size
				Info: map[string]any{
					"hostname":     "redirected.myservicebus.example.org",
					"network-host": "redirected.myservicebus.example.org",
					"port":         uint32(5671),
					"address":      "amqps://redirected.myservicebus.example.org:5671/path",
				},
			},
		},
		ErrCond("the condition"),
		&Error{
			Condition:   ErrCondNotAllowed,
			Description: "foo description",
			Info: map[string]any{
				"other": "info",
				"and":   uint16(875),
			},
		},
		&frames.PerformEnd{
			Error: &Error{
				Condition:   ErrCondNotAllowed,
				Description: "foo description",
				Info: map[string]any{
					"other": "info",
					"and":   uint16(875),
				},
			},
		},
		&frames.PerformClose{
			Error: &Error{
				Condition:   ErrCondNotAllowed,
				Description: "foo description",
				Info: map[string]any{
					"other": "info",
					"and":   uint16(875),
				},
			},
		},
		&Message{
			Header: &MessageHeader{
				Durable:       true,
				Priority:      234,
				TTL:           10 * time.Second,
				FirstAcquirer: true,
				DeliveryCount: 32,
			},
			DeliveryAnnotations: encoding.Annotations{
				int64(42): "answer",
			},
			Annotations: encoding.Annotations{
				int64(42): "answer",
			},
			Properties: &MessageProperties{
				MessageID:          "yo",
				UserID:             []byte("baz"),
				To:                 stringPtr("me"),
				Subject:            stringPtr("sup?"),
				ReplyTo:            stringPtr("you"),
				CorrelationID:      uint64(34513),
				ContentType:        stringPtr("text/plain"),
				ContentEncoding:    stringPtr("UTF-8"),
				AbsoluteExpiryTime: timePtr(time.Date(2018, 01, 13, 14, 24, 07, 0, time.UTC)),
				CreationTime:       timePtr(time.Date(2018, 01, 13, 14, 14, 07, 0, time.UTC)),
				GroupID:            stringPtr("fooGroup"),
				GroupSequence:      uint32Ptr(89324),
				ReplyToGroupID:     stringPtr("barGroup"),
			},
			ApplicationProperties: map[string]any{
				"baz": "foo",
			},
			Data: [][]byte{
				[]byte("A nice little data payload."),
				[]byte("More payload."),
			},
			Value: uint8(42),
			Footer: encoding.Annotations{
				"hash": []uint8{0, 1, 2, 34, 5, 6, 7, 8, 9, 0},
			},
		},
		&Message{
			Header: &MessageHeader{
				Durable:       true,
				Priority:      234,
				TTL:           10 * time.Second,
				FirstAcquirer: true,
				DeliveryCount: 32,
			},
			DeliveryAnnotations: encoding.Annotations{
				int64(42): "answer",
			},
			Annotations: encoding.Annotations{
				int64(42): "answer",
			},
			Properties: &MessageProperties{
				MessageID:          nil,
				UserID:             nil,
				To:                 nil,
				Subject:            nil,
				ReplyTo:            nil,
				CorrelationID:      nil,
				ContentType:        nil,
				ContentEncoding:    nil,
				AbsoluteExpiryTime: nil,
				CreationTime:       nil,
				GroupID:            nil,
				GroupSequence:      nil,
				ReplyToGroupID:     nil,
			},
			ApplicationProperties: map[string]any{
				"baz": "foo",
			},
			Data: [][]byte{
				[]byte("A nice little data payload."),
				[]byte("More payload."),
			},
			Value: uint8(42),
			Footer: encoding.Annotations{
				"hash": []uint8{0, 1, 2, 34, 5, 6, 7, 8, 9, 0},
			},
		},
		&MessageHeader{
			Durable:       true,
			Priority:      234,
			TTL:           10 * time.Second,
			FirstAcquirer: true,
			DeliveryCount: 32,
		},
		&MessageProperties{
			MessageID:          "yo",
			UserID:             []byte("baz"),
			To:                 stringPtr("me"),
			Subject:            stringPtr("sup?"),
			ReplyTo:            stringPtr("you"),
			CorrelationID:      uint64(34513),
			ContentType:        stringPtr("text/plain"),
			ContentEncoding:    stringPtr("UTF-8"),
			AbsoluteExpiryTime: timePtr(time.Date(2018, 01, 13, 14, 24, 07, 0, time.UTC)),
			CreationTime:       timePtr(time.Date(2018, 01, 13, 14, 14, 07, 0, time.UTC)),
			GroupID:            stringPtr("fooGroup"),
			GroupSequence:      uint32Ptr(89324),
			ReplyToGroupID:     stringPtr("barGroup"),
		},
		&encoding.StateReceived{
			SectionNumber: 234,
			SectionOffset: 8973,
		},
		&encoding.StateAccepted{},
		&encoding.StateRejected{
			Error: &Error{
				Condition:   ErrCondStolen,
				Description: "foo description",
				Info: map[string]any{
					"other": "info",
					"and":   int32(uint16(875)),
				},
			},
		},
		&encoding.StateReleased{},
		&encoding.StateModified{
			DeliveryFailed:    true,
			UndeliverableHere: true,
			MessageAnnotations: encoding.Annotations{
				"more": "annotations",
			},
		},
		encoding.LifetimePolicy(encoding.TypeCodeDeleteOnClose),
		SenderSettleMode(1),
		ReceiverSettleMode(1),
		&frames.SASLInit{
			Mechanism:       "FOO",
			InitialResponse: []byte("BAR\x00RESPONSE\x00"),
			Hostname:        "me",
		},
		&frames.SASLMechanisms{
			Mechanisms: []encoding.Symbol{"FOO", "BAR", "BAZ"},
		},
		&frames.SASLChallenge{
			Challenge: []byte("BAR\x00CHALLENGE\x00"),
		},
		&frames.SASLResponse{
			Response: []byte("BAR\x00RESPONSE\x00"),
		},
		&frames.SASLOutcome{
			Code:           encoding.CodeSASLSysPerm,
			AdditionalData: []byte("here's some info for you..."),
		},
		encoding.Milliseconds(10 * time.Second),
		encoding.Symbol("a symbol"),
		map[encoding.Symbol]any{
			"hash": []uint8{0, 1, 2, 34, 5, 6, 7, 8, 9, 0},
		},
	}

	generalTypes = []any{
		nil,
		encoding.UUID{1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16},
		bool(true),
		int8(math.MaxInt8),
		int8(math.MinInt8),
		int16(math.MaxInt16),
		int16(math.MinInt16),
		int32(math.MaxInt32),
		int32(math.MinInt32),
		int64(math.MaxInt64),
		int64(math.MinInt64),
		uint8(math.MaxUint8),
		uint16(math.MaxUint16),
		uint32(math.MaxUint32),
		uint64(math.MaxUint64),
		float32(math.Pi),
		float32(-math.Pi),
		float32(math.NaN()),
		float32(-math.NaN()),
		float64(math.Pi),
		float64(-math.Pi),
		float64(math.NaN()),
		float64(-math.NaN()),
		encoding.DescribedType{
			Descriptor: binary.BigEndian.Uint64([]byte{0x00, 0x00, 0x46, 0x8C, 0x00, 0x00, 0x00, 0x04}),
			Value:      "amqp.annotation.x-opt-offset > '312'",
		},
		map[any]any{
			int32(-1234): []uint8{0, 1, 2, 34, 5, 6, 7, 8, 9, 0},
		},
		map[string]any{
			"hash": []uint8{0, 1, 2, 34, 5, 6, 7, 8, 9, 0},
		},
		encoding.ArrayUByte{1, 2, 3, math.MaxUint8, 0},
		[]int8{1, 2, 3, math.MaxInt8, math.MinInt8},
		[]uint16{1, 2, 3, math.MaxUint16, 0},
		[]uint16{1, 2, 3, math.MaxInt8, 0},
		[]int16{1, 2, 3, math.MaxInt16, math.MinInt16},
		[]int16{1, 2, 3, math.MaxInt8, math.MinInt8},
		[]uint32{1, 2, 3, math.MaxUint32, 0},
		[]uint32{1, 2, 3, math.MaxUint8, 0},
		[]int32{1, 2, 3, math.MaxInt32, math.MinInt32},
		[]int32{1, 2, 3, math.MaxInt8, math.MinInt8},
		[]uint64{1, 2, 3, math.MaxUint64, 0},
		[]uint64{1, 2, 3, math.MaxUint8, 0},
		[]int64{1, 2, 3, math.MaxInt64, math.MinInt64},
		[]int64{1, 2, 3, math.MaxInt8, math.MinInt8},
		[]float32{math.Pi, -math.Pi, float32(math.NaN()), float32(-math.NaN())},
		[]float64{math.Pi, -math.Pi, math.NaN(), -math.NaN()},
		[]bool{true, false, true, false},
		[]string{"FOO", "BAR", "BAZ"},
		[]encoding.Symbol{"FOO", "BAR", "BAZ"},
		[][]byte{[]byte("FOO"), []byte("BAR"), []byte("BAZ")},
		[]time.Time{time.Date(2018, 01, 27, 16, 16, 59, 0, time.UTC)},
		[]encoding.UUID{
			{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 31},
		},
		[]any{int16(1), "hello", false},
		[]map[any]any{
			{
				int32(-1234): []uint8{0, 1, 2, 34, 5, 6, 7, 8, 9, 0},
				"foo":        []uint8{0, 1, 2, 34, 5, 6, 7, 8, 9, 0},
			},
		},
	}
)

func sndSettle(m SenderSettleMode) *SenderSettleMode {
	return &m
}
func rcvSettle(m ReceiverSettleMode) *ReceiverSettleMode {
	return &m
}

func uint32Ptr(u uint32) *uint32 {
	return &u
}
func stringPtr(s string) *string {
	return &s
}

func timePtr(t time.Time) *time.Time {
	return &t
}

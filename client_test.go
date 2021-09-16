package amqp

import (
	"encoding/binary"
	"testing"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
)

func TestLinkOptions(t *testing.T) {
	tests := []struct {
		label string
		opts  []LinkOption

		wantSource     *frames.Source
		wantProperties map[encoding.Symbol]interface{}
	}{
		{
			label: "no options",
		},
		{
			label: "link-filters",
			opts: []LinkOption{
				LinkSelectorFilter("amqp.annotation.x-opt-offset > '100'"),
				LinkProperty("x-opt-test1", "test1"),
				LinkProperty("x-opt-test2", "test2"),
				LinkProperty("x-opt-test1", "test3"),
				LinkPropertyInt64("x-opt-test4", 1),
				LinkPropertyInt32("x-opt-test5", 2),
				LinkSourceFilter("com.microsoft:session-filter", 0x00000137000000C, "123"),
			},

			wantSource: &frames.Source{
				Filter: map[encoding.Symbol]*encoding.DescribedType{
					"apache.org:selector-filter:string": {
						Descriptor: binary.BigEndian.Uint64([]byte{0x00, 0x00, 0x46, 0x8C, 0x00, 0x00, 0x00, 0x04}),
						Value:      "amqp.annotation.x-opt-offset > '100'",
					},
					"com.microsoft:session-filter": {
						Descriptor: binary.BigEndian.Uint64([]byte{0x00, 0x00, 0x00, 0x13, 0x70, 0x00, 0x00, 0x0C}),
						Value:      "123",
					},
				},
			},
			wantProperties: map[encoding.Symbol]interface{}{
				"x-opt-test1": "test3",
				"x-opt-test2": "test2",
				"x-opt-test4": int64(1),
				"x-opt-test5": int32(2),
			},
		},
		{
			label: "more-link-filters",
			opts: []LinkOption{
				LinkSourceFilter("com.microsoft:session-filter", 0x00000137000000C, nil),
			},

			wantSource: &frames.Source{
				Filter: map[encoding.Symbol]*encoding.DescribedType{
					"com.microsoft:session-filter": {
						Descriptor: binary.BigEndian.Uint64([]byte{0x00, 0x00, 0x00, 0x13, 0x70, 0x00, 0x00, 0x0C}),
						Value:      nil,
					},
				},
			},
		},
		{
			label: "link-source-capabilities",
			opts: []LinkOption{
				LinkSourceCapabilities("cap1", "cap2", "cap3"),
			},
			wantSource: &frames.Source{
				Capabilities: []encoding.Symbol{"cap1", "cap2", "cap3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			got, err := newLink(nil, nil, tt.opts)
			if err != nil {
				t.Fatal(err)
			}

			if !testEqual(got.Source, tt.wantSource) {
				t.Errorf("Source properties don't match expected:\n %s", testDiff(got.Source, tt.wantSource))
			}

			if !testEqual(got.properties, tt.wantProperties) {
				t.Errorf("Link properties don't match expected:\n %s", testDiff(got.properties, tt.wantProperties))
			}
		})
	}
}

func TestSourceName(t *testing.T) {
	expectedSourceName := "source-name"
	opts := []LinkOption{
		LinkName(expectedSourceName),
	}

	got, err := newLink(nil, nil, opts)
	if err != nil {
		t.Fatal(err)
	}

	if got.Key.name != expectedSourceName {
		t.Errorf("Link Source Name does not match expected: %v got: %v", expectedSourceName, got.Key.name)
	}
}

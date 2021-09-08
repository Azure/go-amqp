package amqp

import (
	"testing"

	"github.com/Azure/go-amqp/internal/encoding"
)

func TestConnOptions(t *testing.T) {
	tests := []struct {
		label string
		opts  []ConnOption

		wantProperties map[encoding.Symbol]interface{}
	}{
		{
			label: "no options",
		},
		{
			label: "multiple properties",
			opts: []ConnOption{
				ConnProperty("x-opt-test1", "test1"),
				ConnProperty("x-opt-test2", "test2"),
				ConnProperty("x-opt-test1", "test3"),
			},

			wantProperties: map[encoding.Symbol]interface{}{
				"x-opt-test1": "test3",
				"x-opt-test2": "test2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			got, err := newConn(nil, tt.opts...)
			if err != nil {
				t.Fatal(err)
			}

			if !testEqual(got.properties, tt.wantProperties) {
				t.Errorf("Properties don't match expected:\n %s", testDiff(got.properties, tt.wantProperties))
			}
		})
	}
}

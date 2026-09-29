package encoding

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/Azure/go-amqp/internal/buffer"
)

// These tests verify that decoding deeply nested compound values is bounded by
// maxNestingDepth and returns an error.

// describedTypeChain builds a described type nested through the descriptor slot.
func describedTypeChain(levels int) []byte {
	return append([]byte{0x00, 0x53, 0x00}, bytes.Repeat([]byte{0x00}, levels)...)
}

// listChain builds `levels` nested single-element list32 values, exercising the
// list decode path (readAnyList) rather than readComposite.
func listChain(levels int) []byte {
	buf := make([]byte, 0, levels*9+1)
	tmp := make([]byte, 4)
	for k := levels; k >= 1; k-- {
		buf = append(buf, 0xd0) // list32
		binary.BigEndian.PutUint32(tmp, uint32(9*k-4))
		buf = append(buf, tmp...) // size
		binary.BigEndian.PutUint32(tmp, 1)
		buf = append(buf, tmp...) // count = 1
	}
	return append(buf, 0x40) // innermost element: null
}

func assertDepthError(t *testing.T, payload []byte) {
	t.Helper()
	var out any
	err := Unmarshal(buffer.New(payload), &out)
	if err == nil {
		t.Fatalf("expected a nesting-depth error for a %d-byte payload, got nil", len(payload))
	}
	if !strings.Contains(err.Error(), "maximum depth") {
		t.Fatalf("expected a nesting-depth error, got: %v", err)
	}
}

func TestUnmarshalRejectsDeeplyNestedDescribedTypes(t *testing.T) {
	assertDepthError(t, describedTypeChain(1<<20)) // ~1 MB
}

func TestUnmarshalRejectsDeeplyNestedLists(t *testing.T) {
	assertDepthError(t, listChain(maxNestingDepth+50))
}

// A described type whose value is itself a described type is spec-legal, so the
// depth guard (not descriptor validation) is what bounds it.
func TestUnmarshalRejectsDeeplyNestedValues(t *testing.T) {
	assertDepthError(t, bytes.Repeat([]byte{0x00, 0x53, 0x00}, maxNestingDepth+50))
}

func TestUnmarshalAllowsNestingWithinLimit(t *testing.T) {
	// A single described type wrapping a null: well within maxNestingDepth.
	var out any
	if err := Unmarshal(buffer.New([]byte{0x00, 0x53, 0x00, 0x40}), &out); err != nil {
		t.Fatalf("shallow described type should decode without error, got: %v", err)
	}
}

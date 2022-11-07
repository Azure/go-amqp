package amqp

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/buffer"
	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/test"
	"github.com/Azure/go-amqp/internal/testconn"
)

// Known good challenges/responses taken following specification:
// https://developers.google.com/gmail/imap/xoauth2-protocol#the_sasl_xoauth2_mechanism

func TestSaslXOAUTH2InitialResponse(t *testing.T) {
	wantedRespBase64 := "dXNlcj1zb21ldXNlckBleGFtcGxlLmNvbQFhdXRoPUJlYXJlciB5YTI5LnZGOWRmdDRxbVRjMk52YjNSbGNrQmhkSFJoZG1semRHRXVZMjl0Q2cBAQ=="
	wantedResp, err := base64.StdEncoding.DecodeString(wantedRespBase64)
	if err != nil {
		t.Fatal(err)
	}

	gotResp, err := saslXOAUTH2InitialResponse("someuser@example.com", "ya29.vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg")
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(wantedResp, gotResp) {
		t.Errorf("Initial response does not match expected:\n %s", test.Diff(gotResp, wantedResp))
	}
}

// RFC6749 defines the OAUTH2 as comprising VSCHAR elements (\x20-7E)
func TestSaslXOAUTH2InvalidBearer(t *testing.T) {
	tests := []struct {
		label   string
		illegal string
	}{
		{
			label:   "char outside range",
			illegal: "illegalChar\x00",
		},
		{
			label:   "empty bearer",
			illegal: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			_, err := saslXOAUTH2InitialResponse("someuser@example.com", tt.illegal)
			if err == nil {
				t.Errorf("Expected invalid bearer to be rejected")
			}
		})
	}
}

// https://developers.google.com/gmail/imap/xoauth2-protocol gives no guidance on the formation of the username field
// or even if the username is actually required. However, disallowing \x01 seems reasonable as this would interfere
// with the encoding.
func TestSaslXOAUTH2InvalidUsername(t *testing.T) {
	_, err := saslXOAUTH2InitialResponse("illegalChar\x01Within", "ya29.vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg")
	if err == nil {
		t.Errorf("Expected invalid username to be rejected")
	}
}

func TestSaslXOAUTH2EmptyUsername(t *testing.T) {
	_, err := saslXOAUTH2InitialResponse("", "ya29.vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg")
	if err != nil {
		t.Errorf("Expected empty username to be accepted")
	}
}

func TestConnSASLXOAUTH2AuthSuccess(t *testing.T) {
	buf, err := peerResponse(
		[]byte("AMQP\x03\x01\x00\x00"),
		frames.Frame{
			Type:    frames.TypeSASL,
			Channel: 0,
			Body:    &frames.SASLMechanisms{Mechanisms: []encoding.Symbol{saslMechanismXOAUTH2}},
		},
		frames.Frame{
			Type:    frames.TypeSASL,
			Channel: 0,
			Body:    &frames.SASLOutcome{Code: encoding.CodeSASLOK},
		},
		[]byte("AMQP\x00\x01\x00\x00"),
		frames.Frame{
			Type:    frames.TypeAMQP,
			Channel: 0,
			Body:    &frames.PerformOpen{},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	c := testconn.New(buf)
	client, err := New(c, &ConnOptions{
		IdleTimeout: 10 * time.Minute,
		SASLType:    SASLTypeXOAUTH2("someuser@example.com", "ya29.vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg", 512),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
}

func TestConnSASLXOAUTH2AuthFail(t *testing.T) {
	buf, err := peerResponse(
		[]byte("AMQP\x03\x01\x00\x00"),
		frames.Frame{
			Type:    frames.TypeSASL,
			Channel: 0,
			Body:    &frames.SASLMechanisms{Mechanisms: []encoding.Symbol{saslMechanismXOAUTH2}},
		},
		frames.Frame{
			Type:    frames.TypeSASL,
			Channel: 0,
			Body:    &frames.SASLOutcome{Code: encoding.CodeSASLAuth},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	c := testconn.New(buf)
	client, err := New(c, &ConnOptions{
		IdleTimeout: 10 * time.Minute,
		SASLType:    SASLTypeXOAUTH2("someuser@example.com", "ya29.vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg", 512),
	})
	if err == nil {
		defer client.Close()
	}
	switch {
	case err == nil:
		t.Errorf("authentication is expected to fail ")
	case !strings.Contains(err.Error(), fmt.Sprintf("code %#00x", encoding.CodeSASLAuth)):
		t.Errorf("unexpected connection failure : %s", err)
	}
}

func TestConnSASLXOAUTH2AuthFailWithErrorResponse(t *testing.T) {
	buf, err := peerResponse(
		[]byte("AMQP\x03\x01\x00\x00"),
		frames.Frame{
			Type:    frames.TypeSASL,
			Channel: 0,
			Body:    &frames.SASLMechanisms{Mechanisms: []encoding.Symbol{saslMechanismXOAUTH2}},
		},
		frames.Frame{
			Type:    frames.TypeSASL,
			Channel: 0,
			Body:    &frames.SASLChallenge{Challenge: []byte("{ \"status\":\"401\", \"schemes\":\"bearer\", \"scope\":\"https://mail.google.com/\" }")},
		},
		frames.Frame{
			Type:    frames.TypeSASL,
			Channel: 0,
			Body:    &frames.SASLOutcome{Code: encoding.CodeSASLAuth},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	c := testconn.New(buf)
	client, err := New(c, &ConnOptions{
		IdleTimeout: 10 * time.Minute,
		SASLType:    SASLTypeXOAUTH2("someuser@example.com", "ya29.vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg", 512),
	})
	if err == nil {
		defer client.Close()
	}
	switch {
	case err == nil:
		t.Errorf("authentication is expected to fail ")
	case !strings.Contains(err.Error(), fmt.Sprintf("code %#00x", encoding.CodeSASLAuth)):
		t.Errorf("unexpected connection failure : %s", err)
	}
}

func TestConnSASLXOAUTH2AuthFailsAdditionalErrorResponse(t *testing.T) {
	buf, err := peerResponse(
		[]byte("AMQP\x03\x01\x00\x00"),
		frames.Frame{
			Type:    frames.TypeSASL,
			Channel: 0,
			Body:    &frames.SASLMechanisms{Mechanisms: []encoding.Symbol{saslMechanismXOAUTH2}},
		},
		frames.Frame{
			Type:    frames.TypeSASL,
			Channel: 0,
			Body:    &frames.SASLChallenge{Challenge: []byte("fail1")},
		},
		frames.Frame{
			Type:    frames.TypeSASL,
			Channel: 0,
			Body:    &frames.SASLChallenge{Challenge: []byte("fail2")},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	c := testconn.New(buf)
	client, err := New(c, &ConnOptions{
		IdleTimeout: 10 * time.Minute,
		SASLType:    SASLTypeXOAUTH2("someuser@example.com", "ya29.vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg", 512),
	})
	if err == nil {
		defer client.Close()
	}
	switch {
	case err == nil:
		t.Errorf("authentication is expected to fail ")
	case !strings.Contains(err.Error(), "Initial error response: fail1, additional response: fail2"):
		t.Errorf("unexpected connection failure : %s", err)
	}
}

func TestConnSASLExternal(t *testing.T) {
	buf, err := peerResponse(
		[]byte("AMQP\x03\x01\x00\x00"),
		frames.Frame{
			Type:    frames.TypeSASL,
			Channel: 0,
			Body:    &frames.SASLMechanisms{Mechanisms: []encoding.Symbol{saslMechanismEXTERNAL}},
		},
		frames.Frame{
			Type:    frames.TypeSASL,
			Channel: 0,
			Body:    &frames.SASLOutcome{Code: encoding.CodeSASLOK},
		},
		[]byte("AMQP\x00\x01\x00\x00"),
		frames.Frame{
			Type:    frames.TypeAMQP,
			Channel: 0,
			Body:    &frames.PerformOpen{},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	c := testconn.New(buf)
	client, err := New(c, &ConnOptions{
		IdleTimeout: 10 * time.Minute,
		SASLType:    SASLTypeExternal(""),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
}

func peerResponse(items ...interface{}) ([]byte, error) {
	buf := make([]byte, 0)
	for _, item := range items {
		switch v := item.(type) {
		case frames.Frame:
			b := &buffer.Buffer{}
			e := frames.Write(b, v)
			if e != nil {
				return buf, e
			}
			buf = append(buf, b.Bytes()...)
		case []byte:
			buf = append(buf, v...)
		default:
			return buf, fmt.Errorf("unrecongized type %T", item)
		}
	}
	return buf, nil
}

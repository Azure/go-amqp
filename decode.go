package amqp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/Azure/go-amqp/internal/buffer"
)

// parseFrameHeader reads the header from r and returns the result.
//
// No validation is done.
func parseFrameHeader(r *buffer.Buffer) (frameHeader, error) {
	buf, ok := r.Next(8)
	if !ok {
		return frameHeader{}, errors.New("invalid frameHeader")
	}
	_ = buf[7]

	fh := frameHeader{
		Size:       binary.BigEndian.Uint32(buf[0:4]),
		DataOffset: buf[4],
		FrameType:  buf[5],
		Channel:    binary.BigEndian.Uint16(buf[6:8]),
	}

	if fh.Size < frameHeaderSize {
		return fh, fmt.Errorf("received frame header with invalid size %d", fh.Size)
	}

	return fh, nil
}

// parseProtoHeader reads the proto header from r and returns the results
//
// An error is returned if the protocol is not "AMQP" or if the version is not 1.0.0.
func parseProtoHeader(r *buffer.Buffer) (protoHeader, error) {
	const protoHeaderSize = 8
	buf, ok := r.Next(protoHeaderSize)
	if !ok {
		return protoHeader{}, errors.New("invalid protoHeader")
	}
	_ = buf[7]

	if !bytes.Equal(buf[:4], []byte{'A', 'M', 'Q', 'P'}) {
		return protoHeader{}, fmt.Errorf("unexpected protocol %q", buf[:4])
	}

	p := protoHeader{
		ProtoID:  protoID(buf[4]),
		Major:    buf[5],
		Minor:    buf[6],
		Revision: buf[7],
	}

	if p.Major != 1 || p.Minor != 0 || p.Revision != 0 {
		return p, fmt.Errorf("unexpected protocol version %d.%d.%d", p.Major, p.Minor, p.Revision)
	}
	return p, nil
}

// peekFrameBodyType peeks at the frame body's type code without advancing r.
func peekFrameBodyType(r *buffer.Buffer) (amqpType, error) {
	payload := r.Bytes()

	if r.Len() < 3 || payload[0] != 0 || amqpType(payload[1]) != typeCodeSmallUlong {
		return 0, errors.New("invalid frame body header")
	}

	return amqpType(payload[2]), nil
}

// parseFrameBody reads and unmarshals an AMQP frame.
func parseFrameBody(r *buffer.Buffer) (frameBody, error) {
	pType, err := peekFrameBodyType(r)
	if err != nil {
		return nil, err
	}

	switch pType {
	case typeCodeOpen:
		t := new(performOpen)
		err := t.unmarshal(r)
		return t, err
	case typeCodeBegin:
		t := new(performBegin)
		err := t.unmarshal(r)
		return t, err
	case typeCodeAttach:
		t := new(performAttach)
		err := t.unmarshal(r)
		return t, err
	case typeCodeFlow:
		t := new(performFlow)
		err := t.unmarshal(r)
		return t, err
	case typeCodeTransfer:
		t := new(performTransfer)
		err := t.unmarshal(r)
		return t, err
	case typeCodeDisposition:
		t := new(performDisposition)
		err := t.unmarshal(r)
		return t, err
	case typeCodeDetach:
		t := new(performDetach)
		err := t.unmarshal(r)
		return t, err
	case typeCodeEnd:
		t := new(performEnd)
		err := t.unmarshal(r)
		return t, err
	case typeCodeClose:
		t := new(performClose)
		err := t.unmarshal(r)
		return t, err
	case typeCodeSASLMechanism:
		t := new(saslMechanisms)
		err := t.unmarshal(r)
		return t, err
	case typeCodeSASLChallenge:
		t := new(saslChallenge)
		err := t.unmarshal(r)
		return t, err
	case typeCodeSASLOutcome:
		t := new(saslOutcome)
		err := t.unmarshal(r)
		return t, err
	default:
		return nil, fmt.Errorf("unknown preformative type %02x", pType)
	}
}

// unmarshaler is fulfilled by types that can unmarshal
// themselves from AMQP data.
type unmarshaler interface {
	unmarshal(r *buffer.Buffer) error
}

// unmarshal decodes AMQP encoded data into i.
//
// The decoding method is based on the type of i.
//
// If i implements unmarshaler, i.unmarshal() will be called.
//
// Pointers to primitive types will be decoded via the appropriate read[Type] function.
//
// If i is a pointer to a pointer (**Type), it will be dereferenced and a new instance
// of (*Type) is allocated via reflection.
//
// Common map types (map[string]string, map[Symbol]interface{}, and
// map[interface{}]interface{}), will be decoded via conversion to the mapStringAny,
// mapSymbolAny, and mapAnyAny types.
func unmarshal(r *buffer.Buffer, i interface{}) error {
	if tryReadNull(r) {
		return nil
	}

	switch t := i.(type) {
	case *int:
		val, err := readInt(r)
		if err != nil {
			return err
		}
		*t = val
	case *int8:
		val, err := readSbyte(r)
		if err != nil {
			return err
		}
		*t = val
	case *int16:
		val, err := readShort(r)
		if err != nil {
			return err
		}
		*t = val
	case *int32:
		val, err := readInt32(r)
		if err != nil {
			return err
		}
		*t = val
	case *int64:
		val, err := readLong(r)
		if err != nil {
			return err
		}
		*t = val
	case *uint64:
		val, err := readUlong(r)
		if err != nil {
			return err
		}
		*t = val
	case *uint32:
		val, err := readUint32(r)
		if err != nil {
			return err
		}
		*t = val
	case **uint32: // fastpath for uint32 pointer fields
		val, err := readUint32(r)
		if err != nil {
			return err
		}
		*t = &val
	case *uint16:
		val, err := readUshort(r)
		if err != nil {
			return err
		}
		*t = val
	case *uint8:
		val, err := readUbyte(r)
		if err != nil {
			return err
		}
		*t = val
	case *float32:
		val, err := readFloat(r)
		if err != nil {
			return err
		}
		*t = val
	case *float64:
		val, err := readDouble(r)
		if err != nil {
			return err
		}
		*t = val
	case *string:
		val, err := readString(r)
		if err != nil {
			return err
		}
		*t = val
	case *symbol:
		s, err := readString(r)
		if err != nil {
			return err
		}
		*t = symbol(s)
	case *[]byte:
		val, err := readBinary(r)
		if err != nil {
			return err
		}
		*t = val
	case *bool:
		b, err := readBool(r)
		if err != nil {
			return err
		}
		*t = b
	case *time.Time:
		ts, err := readTimestamp(r)
		if err != nil {
			return err
		}
		*t = ts
	case *[]int8:
		return (*arrayInt8)(t).unmarshal(r)
	case *[]uint16:
		return (*arrayUint16)(t).unmarshal(r)
	case *[]int16:
		return (*arrayInt16)(t).unmarshal(r)
	case *[]uint32:
		return (*arrayUint32)(t).unmarshal(r)
	case *[]int32:
		return (*arrayInt32)(t).unmarshal(r)
	case *[]uint64:
		return (*arrayUint64)(t).unmarshal(r)
	case *[]int64:
		return (*arrayInt64)(t).unmarshal(r)
	case *[]float32:
		return (*arrayFloat)(t).unmarshal(r)
	case *[]float64:
		return (*arrayDouble)(t).unmarshal(r)
	case *[]bool:
		return (*arrayBool)(t).unmarshal(r)
	case *[]string:
		return (*arrayString)(t).unmarshal(r)
	case *[]symbol:
		return (*arraySymbol)(t).unmarshal(r)
	case *[][]byte:
		return (*arrayBinary)(t).unmarshal(r)
	case *[]time.Time:
		return (*arrayTimestamp)(t).unmarshal(r)
	case *[]UUID:
		return (*arrayUUID)(t).unmarshal(r)
	case *[]interface{}:
		return (*list)(t).unmarshal(r)
	case *map[interface{}]interface{}:
		return (*mapAnyAny)(t).unmarshal(r)
	case *map[string]interface{}:
		return (*mapStringAny)(t).unmarshal(r)
	case *map[symbol]interface{}:
		return (*mapSymbolAny)(t).unmarshal(r)
	case *deliveryState:
		type_, err := peekMessageType(r.Bytes())
		if err != nil {
			return err
		}

		switch amqpType(type_) {
		case typeCodeStateAccepted:
			*t = new(stateAccepted)
		case typeCodeStateModified:
			*t = new(stateModified)
		case typeCodeStateReceived:
			*t = new(stateReceived)
		case typeCodeStateRejected:
			*t = new(stateRejected)
		case typeCodeStateReleased:
			*t = new(stateReleased)
		default:
			return fmt.Errorf("unexpected type %d for deliveryState", type_)
		}
		return unmarshal(r, *t)

	case *interface{}:
		v, err := readAny(r)
		if err != nil {
			return err
		}
		*t = v

	case unmarshaler:
		return t.unmarshal(r)
	default:
		// handle **T
		v := reflect.Indirect(reflect.ValueOf(i))

		// can't unmarshal into a non-pointer
		if v.Kind() != reflect.Ptr {
			return fmt.Errorf("unable to unmarshal %T", i)
		}

		// if nil pointer, allocate a new value to
		// unmarshal into
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}

		return unmarshal(r, v.Interface())
	}
	return nil
}

// unmarshalComposite is a helper for use in a composite's unmarshal() function.
//
// The composite from r will be unmarshaled into zero or more fields. An error
// will be returned if typ does not match the decoded type.
func unmarshalComposite(r *buffer.Buffer, type_ amqpType, fields ...unmarshalField) error {
	cType, numFields, err := readCompositeHeader(r)
	if err != nil {
		return err
	}

	// check type matches expectation
	if cType != type_ {
		return fmt.Errorf("invalid header %#0x for %#0x", cType, type_)
	}

	// Validate the field count is less than or equal to the number of fields
	// provided. Fields may be omitted by the sender if they are not set.
	if numFields > int64(len(fields)) {
		return fmt.Errorf("invalid field count %d for %#0x", numFields, type_)
	}

	for i, field := range fields[:numFields] {
		// If the field is null and handleNull is set, call it.
		if tryReadNull(r) {
			if field.handleNull != nil {
				err = field.handleNull()
				if err != nil {
					return err
				}
			}
			continue
		}

		// Unmarshal each of the received fields.
		err = unmarshal(r, field.field)
		if err != nil {
			return fmt.Errorf("unmarshaling field %d: %v", i, err)
		}
	}

	// check and call handleNull for the remaining fields
	for _, field := range fields[numFields:] {
		if field.handleNull != nil {
			err = field.handleNull()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// unmarshalField is a struct that contains a field to be unmarshaled into.
//
// An optional nullHandler can be set. If the composite field being unmarshaled
// is null and handleNull is not nil, nullHandler will be called.
type unmarshalField struct {
	field      interface{}
	handleNull nullHandler
}

// nullHandler is a function to be called when a composite's field
// is null.
type nullHandler func() error

func readType(r *buffer.Buffer) (amqpType, error) {
	n, err := r.ReadByte()
	return amqpType(n), err
}

func peekType(r *buffer.Buffer) (amqpType, error) {
	n, err := r.PeekByte()
	return amqpType(n), err
}

// readCompositeHeader reads and consumes the composite header from r.
func readCompositeHeader(r *buffer.Buffer) (_ amqpType, fields int64, _ error) {
	type_, err := readType(r)
	if err != nil {
		return 0, 0, err
	}

	// compsites always start with 0x0
	if type_ != 0 {
		return 0, 0, fmt.Errorf("invalid composite header %#02x", type_)
	}

	// next, the composite type is encoded as an AMQP uint8
	v, err := readUlong(r)
	if err != nil {
		return 0, 0, err
	}

	// fields are represented as a list
	fields, err = readListHeader(r)

	return amqpType(v), fields, err
}

func readListHeader(r *buffer.Buffer) (length int64, _ error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	listLength := r.Len()

	switch type_ {
	case typeCodeList0:
		return 0, nil
	case typeCodeList8:
		buf, ok := r.Next(2)
		if !ok {
			return 0, errors.New("invalid length")
		}
		_ = buf[1]

		size := int(buf[0])
		if size > listLength-1 {
			return 0, errors.New("invalid length")
		}
		length = int64(buf[1])
	case typeCodeList32:
		buf, ok := r.Next(8)
		if !ok {
			return 0, errors.New("invalid length")
		}
		_ = buf[7]

		size := int(binary.BigEndian.Uint32(buf[:4]))
		if size > listLength-4 {
			return 0, errors.New("invalid length")
		}
		length = int64(binary.BigEndian.Uint32(buf[4:8]))
	default:
		return 0, fmt.Errorf("type code %#02x is not a recognized list type", type_)
	}

	return length, nil
}

func readArrayHeader(r *buffer.Buffer) (length int64, _ error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	arrayLength := r.Len()

	switch type_ {
	case typeCodeArray8:
		buf, ok := r.Next(2)
		if !ok {
			return 0, errors.New("invalid length")
		}
		_ = buf[1]

		size := int(buf[0])
		if size > arrayLength-1 {
			return 0, errors.New("invalid length")
		}
		length = int64(buf[1])
	case typeCodeArray32:
		buf, ok := r.Next(8)
		if !ok {
			return 0, errors.New("invalid length")
		}
		_ = buf[7]

		size := binary.BigEndian.Uint32(buf[:4])
		if int(size) > arrayLength-4 {
			return 0, fmt.Errorf("invalid length for type %02x", type_)
		}
		length = int64(binary.BigEndian.Uint32(buf[4:8]))
	default:
		return 0, fmt.Errorf("type code %#02x is not a recognized array type", type_)
	}
	return length, nil
}

func readString(r *buffer.Buffer) (string, error) {
	type_, err := readType(r)
	if err != nil {
		return "", err
	}

	var length int64
	switch type_ {
	case typeCodeStr8, typeCodeSym8:
		n, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		length = int64(n)
	case typeCodeStr32, typeCodeSym32:
		buf, ok := r.Next(4)
		if !ok {
			return "", fmt.Errorf("invalid length for type %#02x", type_)
		}
		length = int64(binary.BigEndian.Uint32(buf))
	default:
		return "", fmt.Errorf("type code %#02x is not a recognized string type", type_)
	}

	buf, ok := r.Next(length)
	if !ok {
		return "", errors.New("invalid length")
	}
	return string(buf), nil
}

func readBinary(r *buffer.Buffer) ([]byte, error) {
	type_, err := readType(r)
	if err != nil {
		return nil, err
	}

	var length int64
	switch type_ {
	case typeCodeVbin8:
		n, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		length = int64(n)
	case typeCodeVbin32:
		buf, ok := r.Next(4)
		if !ok {
			return nil, fmt.Errorf("invalid length for type %#02x", type_)
		}
		length = int64(binary.BigEndian.Uint32(buf))
	default:
		return nil, fmt.Errorf("type code %#02x is not a recognized binary type", type_)
	}

	if length == 0 {
		// An empty value and a nil value are distinct,
		// ensure that the returned value is not nil in this case.
		return make([]byte, 0), nil
	}

	buf, ok := r.Next(length)
	if !ok {
		return nil, errors.New("invalid length")
	}
	return append([]byte(nil), buf...), nil
}

func readAny(r *buffer.Buffer) (interface{}, error) {
	if tryReadNull(r) {
		return nil, nil
	}

	type_, err := peekType(r)
	if err != nil {
		return nil, errors.New("invalid length")
	}

	switch type_ {
	// composite
	case 0x0:
		return readComposite(r)

	// bool
	case typeCodeBool, typeCodeBoolTrue, typeCodeBoolFalse:
		return readBool(r)

	// uint
	case typeCodeUbyte:
		return readUbyte(r)
	case typeCodeUshort:
		return readUshort(r)
	case typeCodeUint,
		typeCodeSmallUint,
		typeCodeUint0:
		return readUint32(r)
	case typeCodeUlong,
		typeCodeSmallUlong,
		typeCodeUlong0:
		return readUlong(r)

	// int
	case typeCodeByte:
		return readSbyte(r)
	case typeCodeShort:
		return readShort(r)
	case typeCodeInt,
		typeCodeSmallint:
		return readInt32(r)
	case typeCodeLong,
		typeCodeSmalllong:
		return readLong(r)

	// floating point
	case typeCodeFloat:
		return readFloat(r)
	case typeCodeDouble:
		return readDouble(r)

	// binary
	case typeCodeVbin8, typeCodeVbin32:
		return readBinary(r)

	// strings
	case typeCodeStr8, typeCodeStr32:
		return readString(r)
	case typeCodeSym8, typeCodeSym32:
		// symbols currently decoded as string to avoid
		// exposing symbol type in message, this may need
		// to change if users need to distinguish strings
		// from symbols
		return readString(r)

	// timestamp
	case typeCodeTimestamp:
		return readTimestamp(r)

	// UUID
	case typeCodeUUID:
		return readUUID(r)

	// arrays
	case typeCodeArray8, typeCodeArray32:
		return readAnyArray(r)

	// lists
	case typeCodeList0, typeCodeList8, typeCodeList32:
		return readAnyList(r)

	// maps
	case typeCodeMap8:
		return readAnyMap(r)
	case typeCodeMap32:
		return readAnyMap(r)

	// TODO: implement
	case typeCodeDecimal32:
		return nil, errors.New("decimal32 not implemented")
	case typeCodeDecimal64:
		return nil, errors.New("decimal64 not implemented")
	case typeCodeDecimal128:
		return nil, errors.New("decimal128 not implemented")
	case typeCodeChar:
		return nil, errors.New("char not implemented")
	default:
		return nil, fmt.Errorf("unknown type %#02x", type_)
	}
}

func readAnyMap(r *buffer.Buffer) (interface{}, error) {
	var m map[interface{}]interface{}
	err := (*mapAnyAny)(&m).unmarshal(r)
	if err != nil {
		return nil, err
	}

	if len(m) == 0 {
		return m, nil
	}

	stringKeys := true
Loop:
	for key := range m {
		switch key.(type) {
		case string:
		case symbol:
		default:
			stringKeys = false
			break Loop
		}
	}

	if stringKeys {
		mm := make(map[string]interface{}, len(m))
		for key, value := range m {
			switch key := key.(type) {
			case string:
				mm[key] = value
			case symbol:
				mm[string(key)] = value
			}
		}
		return mm, nil
	}

	return m, nil
}

func readAnyList(r *buffer.Buffer) (interface{}, error) {
	var a []interface{}
	err := (*list)(&a).unmarshal(r)
	return a, err
}

func readAnyArray(r *buffer.Buffer) (interface{}, error) {
	// get the array type
	buf := r.Bytes()
	if len(buf) < 1 {
		return nil, errors.New("invalid length")
	}

	var typeIdx int
	switch amqpType(buf[0]) {
	case typeCodeArray8:
		typeIdx = 3
	case typeCodeArray32:
		typeIdx = 9
	default:
		return nil, fmt.Errorf("invalid array type %02x", buf[0])
	}
	if len(buf) < typeIdx+1 {
		return nil, errors.New("invalid length")
	}

	switch amqpType(buf[typeIdx]) {
	case typeCodeByte:
		var a []int8
		err := (*arrayInt8)(&a).unmarshal(r)
		return a, err
	case typeCodeUbyte:
		var a ArrayUByte
		err := a.unmarshal(r)
		return a, err
	case typeCodeUshort:
		var a []uint16
		err := (*arrayUint16)(&a).unmarshal(r)
		return a, err
	case typeCodeShort:
		var a []int16
		err := (*arrayInt16)(&a).unmarshal(r)
		return a, err
	case typeCodeUint0, typeCodeSmallUint, typeCodeUint:
		var a []uint32
		err := (*arrayUint32)(&a).unmarshal(r)
		return a, err
	case typeCodeSmallint, typeCodeInt:
		var a []int32
		err := (*arrayInt32)(&a).unmarshal(r)
		return a, err
	case typeCodeUlong0, typeCodeSmallUlong, typeCodeUlong:
		var a []uint64
		err := (*arrayUint64)(&a).unmarshal(r)
		return a, err
	case typeCodeSmalllong, typeCodeLong:
		var a []int64
		err := (*arrayInt64)(&a).unmarshal(r)
		return a, err
	case typeCodeFloat:
		var a []float32
		err := (*arrayFloat)(&a).unmarshal(r)
		return a, err
	case typeCodeDouble:
		var a []float64
		err := (*arrayDouble)(&a).unmarshal(r)
		return a, err
	case typeCodeBool, typeCodeBoolTrue, typeCodeBoolFalse:
		var a []bool
		err := (*arrayBool)(&a).unmarshal(r)
		return a, err
	case typeCodeStr8, typeCodeStr32:
		var a []string
		err := (*arrayString)(&a).unmarshal(r)
		return a, err
	case typeCodeSym8, typeCodeSym32:
		var a []symbol
		err := (*arraySymbol)(&a).unmarshal(r)
		return a, err
	case typeCodeVbin8, typeCodeVbin32:
		var a [][]byte
		err := (*arrayBinary)(&a).unmarshal(r)
		return a, err
	case typeCodeTimestamp:
		var a []time.Time
		err := (*arrayTimestamp)(&a).unmarshal(r)
		return a, err
	case typeCodeUUID:
		var a []UUID
		err := (*arrayUUID)(&a).unmarshal(r)
		return a, err
	default:
		return nil, fmt.Errorf("array decoding not implemented for %#02x", buf[typeIdx])
	}
}

func readComposite(r *buffer.Buffer) (interface{}, error) {
	buf := r.Bytes()

	if len(buf) < 2 {
		return nil, errors.New("invalid length for composite")
	}

	// compsites start with 0x0
	if amqpType(buf[0]) != 0x0 {
		return nil, fmt.Errorf("invalid composite header %#02x", buf[0])
	}

	var compositeType uint64
	switch amqpType(buf[1]) {
	case typeCodeSmallUlong:
		if len(buf) < 3 {
			return nil, errors.New("invalid length for smallulong")
		}
		compositeType = uint64(buf[2])
	case typeCodeUlong:
		if len(buf) < 10 {
			return nil, errors.New("invalid length for ulong")
		}
		compositeType = binary.BigEndian.Uint64(buf[2:])
	}

	if compositeType > math.MaxUint8 {
		// try as described type
		var dt describedType
		err := dt.unmarshal(r)
		return dt, err
	}

	switch amqpType(compositeType) {
	// Error
	case typeCodeError:
		t := new(Error)
		err := t.unmarshal(r)
		return t, err

	// Lifetime Policies
	case typeCodeDeleteOnClose:
		t := deleteOnClose
		err := t.unmarshal(r)
		return t, err
	case typeCodeDeleteOnNoMessages:
		t := deleteOnNoMessages
		err := t.unmarshal(r)
		return t, err
	case typeCodeDeleteOnNoLinks:
		t := deleteOnNoLinks
		err := t.unmarshal(r)
		return t, err
	case typeCodeDeleteOnNoLinksOrMessages:
		t := deleteOnNoLinksOrMessages
		err := t.unmarshal(r)
		return t, err

	// Delivery States
	case typeCodeStateAccepted:
		t := new(stateAccepted)
		err := t.unmarshal(r)
		return t, err
	case typeCodeStateModified:
		t := new(stateModified)
		err := t.unmarshal(r)
		return t, err
	case typeCodeStateReceived:
		t := new(stateReceived)
		err := t.unmarshal(r)
		return t, err
	case typeCodeStateRejected:
		t := new(stateRejected)
		err := t.unmarshal(r)
		return t, err
	case typeCodeStateReleased:
		t := new(stateReleased)
		err := t.unmarshal(r)
		return t, err

	case typeCodeOpen,
		typeCodeBegin,
		typeCodeAttach,
		typeCodeFlow,
		typeCodeTransfer,
		typeCodeDisposition,
		typeCodeDetach,
		typeCodeEnd,
		typeCodeClose,
		typeCodeSource,
		typeCodeTarget,
		typeCodeMessageHeader,
		typeCodeDeliveryAnnotations,
		typeCodeMessageAnnotations,
		typeCodeMessageProperties,
		typeCodeApplicationProperties,
		typeCodeApplicationData,
		typeCodeAMQPSequence,
		typeCodeAMQPValue,
		typeCodeFooter,
		typeCodeSASLMechanism,
		typeCodeSASLInit,
		typeCodeSASLChallenge,
		typeCodeSASLResponse,
		typeCodeSASLOutcome:
		return nil, fmt.Errorf("readComposite unmarshal not implemented for %#02x", compositeType)

	default:
		// try as described type
		var dt describedType
		err := dt.unmarshal(r)
		return dt, err
	}
}

func readTimestamp(r *buffer.Buffer) (time.Time, error) {
	type_, err := readType(r)
	if err != nil {
		return time.Time{}, err
	}

	if type_ != typeCodeTimestamp {
		return time.Time{}, fmt.Errorf("invalid type for timestamp %02x", type_)
	}

	n, err := r.ReadUint64()
	ms := int64(n)
	return time.Unix(ms/1000, (ms%1000)*1000000).UTC(), err
}

func readInt(r *buffer.Buffer) (int, error) {
	type_, err := peekType(r)
	if err != nil {
		return 0, err
	}

	switch type_ {
	// Unsigned
	case typeCodeUbyte:
		n, err := readUbyte(r)
		return int(n), err
	case typeCodeUshort:
		n, err := readUshort(r)
		return int(n), err
	case typeCodeUint0, typeCodeSmallUint, typeCodeUint:
		n, err := readUint32(r)
		return int(n), err
	case typeCodeUlong0, typeCodeSmallUlong, typeCodeUlong:
		n, err := readUlong(r)
		return int(n), err

	// Signed
	case typeCodeByte:
		n, err := readSbyte(r)
		return int(n), err
	case typeCodeShort:
		n, err := readShort(r)
		return int(n), err
	case typeCodeSmallint, typeCodeInt:
		n, err := readInt32(r)
		return int(n), err
	case typeCodeSmalllong, typeCodeLong:
		n, err := readLong(r)
		return int(n), err
	default:
		return 0, fmt.Errorf("type code %#02x is not a recognized number type", type_)
	}
}

func readLong(r *buffer.Buffer) (int64, error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	switch type_ {
	case typeCodeSmalllong:
		n, err := r.ReadByte()
		return int64(n), err
	case typeCodeLong:
		n, err := r.ReadUint64()
		return int64(n), err
	default:
		return 0, fmt.Errorf("invalid type for uint32 %02x", type_)
	}
}

func readInt32(r *buffer.Buffer) (int32, error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	switch type_ {
	case typeCodeSmallint:
		n, err := r.ReadByte()
		return int32(n), err
	case typeCodeInt:
		n, err := r.ReadUint32()
		return int32(n), err
	default:
		return 0, fmt.Errorf("invalid type for int32 %02x", type_)
	}
}

func readShort(r *buffer.Buffer) (int16, error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	if type_ != typeCodeShort {
		return 0, fmt.Errorf("invalid type for short %02x", type_)
	}

	n, err := r.ReadUint16()
	return int16(n), err
}

func readSbyte(r *buffer.Buffer) (int8, error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	if type_ != typeCodeByte {
		return 0, fmt.Errorf("invalid type for int8 %02x", type_)
	}

	n, err := r.ReadByte()
	return int8(n), err
}

func readUbyte(r *buffer.Buffer) (uint8, error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	if type_ != typeCodeUbyte {
		return 0, fmt.Errorf("invalid type for ubyte %02x", type_)
	}

	return r.ReadByte()
}

func readUshort(r *buffer.Buffer) (uint16, error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	if type_ != typeCodeUshort {
		return 0, fmt.Errorf("invalid type for ushort %02x", type_)
	}

	return r.ReadUint16()
}

func readUint32(r *buffer.Buffer) (uint32, error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	switch type_ {
	case typeCodeUint0:
		return 0, nil
	case typeCodeSmallUint:
		n, err := r.ReadByte()
		return uint32(n), err
	case typeCodeUint:
		return r.ReadUint32()
	default:
		return 0, fmt.Errorf("invalid type for uint32 %02x", type_)
	}
}

func readUlong(r *buffer.Buffer) (uint64, error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	switch type_ {
	case typeCodeUlong0:
		return 0, nil
	case typeCodeSmallUlong:
		n, err := r.ReadByte()
		return uint64(n), err
	case typeCodeUlong:
		return r.ReadUint64()
	default:
		return 0, fmt.Errorf("invalid type for uint32 %02x", type_)
	}
}

func readFloat(r *buffer.Buffer) (float32, error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	if type_ != typeCodeFloat {
		return 0, fmt.Errorf("invalid type for float32 %02x", type_)
	}

	bits, err := r.ReadUint32()
	return math.Float32frombits(bits), err
}

func readDouble(r *buffer.Buffer) (float64, error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	if type_ != typeCodeDouble {
		return 0, fmt.Errorf("invalid type for float64 %02x", type_)
	}

	bits, err := r.ReadUint64()
	return math.Float64frombits(bits), err
}

func readBool(r *buffer.Buffer) (bool, error) {
	type_, err := readType(r)
	if err != nil {
		return false, err
	}

	switch type_ {
	case typeCodeBool:
		b, err := r.ReadByte()
		return b != 0, err
	case typeCodeBoolTrue:
		return true, nil
	case typeCodeBoolFalse:
		return false, nil
	default:
		return false, fmt.Errorf("type code %#02x is not a recognized bool type", type_)
	}
}

func readUint(r *buffer.Buffer) (value uint64, _ error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	switch type_ {
	case typeCodeUint0, typeCodeUlong0:
		return 0, nil
	case typeCodeUbyte, typeCodeSmallUint, typeCodeSmallUlong:
		n, err := r.ReadByte()
		return uint64(n), err
	case typeCodeUshort:
		n, err := r.ReadUint16()
		return uint64(n), err
	case typeCodeUint:
		n, err := r.ReadUint32()
		return uint64(n), err
	case typeCodeUlong:
		return r.ReadUint64()
	default:
		return 0, fmt.Errorf("type code %#02x is not a recognized number type", type_)
	}
}

func readUUID(r *buffer.Buffer) (UUID, error) {
	var uuid UUID

	type_, err := readType(r)
	if err != nil {
		return uuid, err
	}

	if type_ != typeCodeUUID {
		return uuid, fmt.Errorf("type code %#00x is not a UUID", type_)
	}

	buf, ok := r.Next(16)
	if !ok {
		return uuid, errors.New("invalid length")
	}
	copy(uuid[:], buf)

	return uuid, nil
}

func readMapHeader(r *buffer.Buffer) (count uint32, _ error) {
	type_, err := readType(r)
	if err != nil {
		return 0, err
	}

	length := r.Len()

	switch type_ {
	case typeCodeMap8:
		buf, ok := r.Next(2)
		if !ok {
			return 0, errors.New("invalid length")
		}
		_ = buf[1]

		size := int(buf[0])
		if size > length-1 {
			return 0, errors.New("invalid length")
		}
		count = uint32(buf[1])
	case typeCodeMap32:
		buf, ok := r.Next(8)
		if !ok {
			return 0, errors.New("invalid length")
		}
		_ = buf[7]

		size := int(binary.BigEndian.Uint32(buf[:4]))
		if size > length-4 {
			return 0, errors.New("invalid length")
		}
		count = binary.BigEndian.Uint32(buf[4:8])
	default:
		return 0, fmt.Errorf("invalid map type %#02x", type_)
	}

	if int(count) > r.Len() {
		return 0, errors.New("invalid length")
	}
	return count, nil
}

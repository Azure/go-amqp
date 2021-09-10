package encoding

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/buffer"
)

func fuzzUnmarshal(data []byte) int {
	types := []interface{}{
		new(Error),
		new(*Error),
		new(StateReceived),
		new(*StateReceived),
		new(StateAccepted),
		new(*StateAccepted),
		new(StateRejected),
		new(*StateRejected),
		new(StateReleased),
		new(*StateReleased),
		new(StateModified),
		new(*StateModified),
		new(mapAnyAny),
		new(*mapAnyAny),
		new(mapStringAny),
		new(*mapStringAny),
		new(mapSymbolAny),
		new(*mapSymbolAny),
		new(Unsettled),
		new(*Unsettled),
		new(Milliseconds),
		new(*Milliseconds),
		new(bool),
		new(*bool),
		new(int8),
		new(*int8),
		new(int16),
		new(*int16),
		new(int32),
		new(*int32),
		new(int64),
		new(*int64),
		new(uint8),
		new(*uint8),
		new(uint16),
		new(*uint16),
		new(uint32),
		new(*uint32),
		new(uint64),
		new(*uint64),
		new(time.Time),
		new(*time.Time),
		new(time.Duration),
		new(*time.Duration),
		new(Symbol),
		new(*Symbol),
		new([]byte),
		new(*[]byte),
		new([]string),
		new(*[]string),
		new([]Symbol),
		new(*[]Symbol),
		new(map[interface{}]interface{}),
		new(*map[interface{}]interface{}),
		new(map[string]interface{}),
		new(*map[string]interface{}),
		new(map[Symbol]interface{}),
		new(*map[Symbol]interface{}),
		new(interface{}),
		new(*interface{}),
		new(ErrorCondition),
		new(*ErrorCondition),
		new(UUID),
		new(*UUID),
		new(Role),
		new(*Role),
	}

	for _, t := range types {
		_ = Unmarshal(buffer.New(data), t)
		_, _ = ReadAny(buffer.New(data))
	}
	return 0
}

func TestFuzzMarshalCrashers(t *testing.T) {
	tests := []string{
		0:  "\xc1\x000\xa0\x00S0",
		1:  "\xf0S\x13\xc0\x12\v@`@@`\v@```@@@",
		2:  "\xe000\xb0",
		3:  "\xc1\x000\xe000R",
		4:  "\xe000S",
		5:  "\x00\xe000R",
		6:  "\xe000\x83",
		7:  "\x00\x00\xe000S",
		8:  "\xe000R",
		9:  "\x00\xe000S",
		10: "\xc1\x000\xe000S",
		11: "\xc1\x000\x00\xe000S",
		12: "\xc1\x000\x00\xe000R",
		13: "\x00\x00\xe000R",
		14: "\xe000\xb1",
		15: "\xc1\x00%\xd0\x00\x00\x00M\xe2\x00\x00\x01\x00S\x1d\xd0\x00\x00\x00A" +
			"\x00\x00\x00\x03\xa3\x10amqp:link:stol" +
			"en\xa1\x0foo\xb1\xdefoo descript" +
			"ion\xc1\x18\x04\xa1\x05other\xa1\x04info\xa1" +
			"\x03andq\x00\x00\x03k",
		16: "\xd1\x00\x00\x00M\x00S\x1d\xd0\x00S\x1d\xd0\x00\x00\x00A\x00\x80\x00" +
			"\x03\xa3\x10amqp:link:stolen\xa1" +
			"\x19foo description\xc1\x18\x04\xa1" +
			"\x05other\xa1\x04info\xa1\x03andU\x00\x00" +
			"\x03k",
		17: "\xf0\x00\x00\x00\x01@\x00TRUE\x00",
		18: "\xf0\x00\x00\x00\x00\x10RTRT",
		19: "\x00p\x00inp\xf0\x00\x00\x00\x01p\x00inp",
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			fuzzUnmarshal([]byte(tt))
		})
	}
}

func testDirFiles(t *testing.T, dir string) []string {
	finfos, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	var fullpaths []string
	for _, finfo := range finfos {
		fullpaths = append(fullpaths, filepath.Join(dir, finfo.Name()))
	}

	return fullpaths
}

func TestFuzzMarshalCorpus(t *testing.T) {
	if os.Getenv("TEST_CORPUS") == "" {
		t.Skip("set TEST_CORPUS to enable")
	}

	for _, path := range testDirFiles(t, "testdata/fuzz/marshal/corpus") {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := ioutil.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			fuzzUnmarshal(data)
		})
	}
}

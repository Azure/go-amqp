package test

import (
	"reflect"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func Equal(x, y any) bool {
	return cmp.Equal(x, y, compareOpts(x, y)...)
}

func Diff(x, y any) string {
	return cmp.Diff(x, y, compareOpts(x, y)...)
}

func compareOpts(x, y any) []cmp.Option {
	return cmp.Options{
		deepAllowUnexported(x, y),
		cmpopts.EquateNaNs(),
	}
}

// from https://github.com/google/go-cmp/issues/40
func deepAllowUnexported(vs ...any) cmp.Option {
	m := make(map[reflect.Type]struct{})
	for _, v := range vs {
		structTypes(reflect.ValueOf(v), m)
	}
	var types []any
	for t := range m {
		types = append(types, reflect.New(t).Elem().Interface())
	}
	return cmp.AllowUnexported(types...)
}

func structTypes(v reflect.Value, m map[reflect.Type]struct{}) {
	if !v.IsValid() {
		return
	}
	switch v.Kind() {
	case reflect.Ptr:
		if !v.IsNil() {
			structTypes(v.Elem(), m)
		}
	case reflect.Interface:
		if !v.IsNil() {
			structTypes(v.Elem(), m)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			structTypes(v.Index(i), m)
		}
	case reflect.Map:
		for _, k := range v.MapKeys() {
			structTypes(v.MapIndex(k), m)
		}
	case reflect.Struct:
		m[v.Type()] = struct{}{}
		for i := 0; i < v.NumField(); i++ {
			structTypes(v.Field(i), m)
		}
	}
}

// MuxSemaphore is a helper for managing rendezvous with the mux at certain points during execution.
type MuxSemaphore struct {
	count    int
	pausedCh chan struct{}
	resumeCh chan struct{}
}

// NewMuxSemaphore creates a new MuxSemaphore with the specified count.
//   - count is the number of iterations the mux will execute before pausing
func NewMuxSemaphore(count int) *MuxSemaphore {
	return &MuxSemaphore{
		count:    count,
		pausedCh: make(chan struct{}),
		resumeCh: make(chan struct{}),
	}
}

// OnLoop is to be called from your mux test hook.
// panics if the pause/resume cycle takes more than 5s.
func (m *MuxSemaphore) OnLoop() {
	if m.count > 0 {
		m.count--
		return
	} else if m.count < 0 {
		return
	}

	timer := time.NewTimer(5 * time.Second)

	select {
	case m.pausedCh <- struct{}{}:
		// mux is now paused
	case <-timer.C:
		panic("pause time exceeded")
	}

	select {
	case <-m.resumeCh:
		// mux resumed
	case <-timer.C:
		panic("resume time exceeded")
	}

	timer.Stop()
}

// Wait - blocks until the mux is paused.
// panics if the wait exceeds 5s.
func (m *MuxSemaphore) Wait() {
	select {
	case <-m.pausedCh:
		// mux is paused
	case <-time.After(5 * time.Second):
		panic("wait time exceeded")
	}
}

// Release - call this to resume the mux with a new count (zero-based)
//   - count is the number of iterations the mux will execute before pausing, pass -1 to close the semaphore
//
// panics if the wait exceeds 5s.
func (m *MuxSemaphore) Release(count int) {
	select {
	case m.resumeCh <- struct{}{}:
		m.count = count
	case <-time.After(5 * time.Second):
		panic("release time exceeded")
	}
}

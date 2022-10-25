//go:build !debug
// +build !debug

package debug

// dummy functions used when debugging is not enabled

func Log(_ int, _ string, _ ...interface{}) {}

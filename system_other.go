//go:build !linux

package db

import "runtime"

// SystemResources holds detected system resource information.
type SystemResources struct {
	TotalRAM uint64 // Total system RAM in bytes
	NumCPU   int    // Number of logical CPUs
}

// GetSystemResources returns system resource information.
// On non-Linux platforms, TotalRAM returns 0 (fallback to default values).
func GetSystemResources() SystemResources {
	return SystemResources{
		TotalRAM: 0, // Will trigger fallback to default cache size
		NumCPU:   runtime.NumCPU(),
	}
}

// Package version exposes build metadata for Payment Rail binaries.
//
// The exported vars are overridden at link time via -ldflags (see the
// Makefile's LDFLAGS). At `go run` / test time they keep their dev defaults.
package version

import "fmt"

var (
	// Version is the semantic version or git describe of the build.
	Version = "dev"
	// Commit is the short git SHA the binary was built from.
	Commit = "none"
	// BuildDate is the UTC build timestamp (RFC 3339).
	BuildDate = "unknown"
)

// String returns a single-line, human-readable build identifier.
func String() string {
	return format(Version, Commit, BuildDate)
}

// format is separated from String so it can be tested without mutating the
// package-level build vars.
func format(version, commit, buildDate string) string {
	return fmt.Sprintf("%s (commit %s, built %s)", version, commit, buildDate)
}

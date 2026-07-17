package version

import "testing"

// TestFormat is the repo's canonical table-driven test example (M0 learning
// objective). New tests should follow this shape: a slice of named cases run
// under t.Run so failures point at the exact case.
func TestFormat(t *testing.T) {
	tests := []struct {
		name    string
		version string
		commit  string
		date    string
		want    string
	}{
		{
			name:    "dev defaults",
			version: "dev",
			commit:  "none",
			date:    "unknown",
			want:    "dev (commit none, built unknown)",
		},
		{
			name:    "tagged release",
			version: "v1.2.3",
			commit:  "abc1234",
			date:    "2026-07-16T00:00:00Z",
			want:    "v1.2.3 (commit abc1234, built 2026-07-16T00:00:00Z)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := format(tt.version, tt.commit, tt.date); got != tt.want {
				t.Errorf("format(%q, %q, %q) = %q, want %q",
					tt.version, tt.commit, tt.date, got, tt.want)
			}
		})
	}
}

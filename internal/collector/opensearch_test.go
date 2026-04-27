package collector

import "testing"

func TestParseSizeGB(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		want  float64
		delta float64
	}{
		{name: "plain_gb", in: "42", want: 42, delta: 0.000001},
		{name: "gb_suffix", in: "12.5gb", want: 12.5, delta: 0.000001},
		{name: "tb_suffix", in: "1tb", want: 1024, delta: 0.000001},
		{name: "mb_suffix", in: "1024mb", want: 1, delta: 0.000001},
		{name: "bytes_suffix", in: "1073741824b", want: 1, delta: 0.000001},
		{name: "dash", in: "-", want: 0, delta: 0.000001},
		{name: "empty", in: "", want: 0, delta: 0.000001},
		{name: "invalid", in: "n/a", want: 0, delta: 0.000001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseSizeGB(tt.in)
			if abs(got-tt.want) > tt.delta {
				t.Fatalf("parseSizeGB(%q) = %f, want %f", tt.in, got, tt.want)
			}
		})
	}
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

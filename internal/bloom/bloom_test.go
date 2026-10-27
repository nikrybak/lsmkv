package bloom

import (
	"fmt"
	"testing"
)

func TestNoFalseNegatives(t *testing.T) {
	for _, n := range []int{0, 1, 10, 1000, 50000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			b := NewBuilder(10)
			for i := 0; i < n; i++ {
				b.Add([]byte(fmt.Sprintf("key-%d", i)))
			}
			f := b.Finish()
			for i := 0; i < n; i++ {
				if !MayContain(f, []byte(fmt.Sprintf("key-%d", i))) {
					t.Fatalf("false negative for key-%d", i)
				}
			}
		})
	}
}

func TestFalsePositiveRate(t *testing.T) {
	tests := []struct {
		bitsPerKey int
		maxRate    float64
	}{
		{5, 0.15},
		{10, 0.02},
		{20, 0.002},
	}
	const n = 20000
	for _, tt := range tests {
		t.Run(fmt.Sprintf("bits=%d", tt.bitsPerKey), func(t *testing.T) {
			b := NewBuilder(tt.bitsPerKey)
			for i := 0; i < n; i++ {
				b.Add([]byte(fmt.Sprintf("present-%d", i)))
			}
			f := b.Finish()
			fp := 0
			for i := 0; i < n; i++ {
				if MayContain(f, []byte(fmt.Sprintf("absent-%d", i))) {
					fp++
				}
			}
			if rate := float64(fp) / n; rate > tt.maxRate {
				t.Fatalf("false positive rate %.4f > %.4f", rate, tt.maxRate)
			}
		})
	}
}

func TestMalformedFilterIsPermissive(t *testing.T) {
	for _, f := range [][]byte{nil, {0}, {0xff, 0}, {0xff, 31}} {
		if !MayContain(f, []byte("k")) {
			t.Fatalf("MayContain(%v) = false, want true", f)
		}
	}
}

func BenchmarkMayContain(b *testing.B) {
	bl := NewBuilder(10)
	for i := 0; i < 100000; i++ {
		bl.Add([]byte(fmt.Sprintf("key-%d", i)))
	}
	f := bl.Finish()
	key := []byte("key-12345")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MayContain(f, key)
	}
}

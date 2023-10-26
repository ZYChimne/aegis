package hotkey

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

func benchmarkHotkey(b *testing.B, autoCache bool, writePercent float64, whilelist ...*CacheRuleConfig) {
	option := &Option{
		HotKeyCnt:     100,
		LocalCacheCap: 100,
		AutoCache:     autoCache,
		TTL:           100 * time.Millisecond,
		WhileList:     whilelist,
	}

	h, err := NewHotkey(option)
	if err != nil {
		b.Fatalf("new hot key failed,err:=%v", err)
	}
	random := rand.New(rand.NewSource(uint64(time.Now().Unix())))
	zipf := rand.NewZipf(rand.New(rand.NewSource(uint64(time.Now().Unix()))), 2, 2, 1000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := strconv.FormatUint(zipf.Uint64(), 10)
			if random.Float64() < writePercent {
				h.AddWithValue(key, key, 1)
			} else {
				h.Get(key)
			}
		}
	})
}

func BenchmarkHotkeyAutoCacheWrite1_100(b *testing.B) {
	benchmarkHotkey(b, true, 0.01)
}

func BenchmarkHotkeyAutoCacheWrite10_100(b *testing.B) {
	benchmarkHotkey(b, true, 0.1)
}

func BenchmarkHotkeyAutoCacheWrite50_100(b *testing.B) {
	benchmarkHotkey(b, true, 0.5)
}

func BenchmarkHotkeyFading(b *testing.B) {
	option := &Option{
		HotKeyCnt:     6000,
		LocalCacheCap: 100,
		AutoCache:     true,
		TTL:           100*time.Millisecond,
	}

	h, err := NewHotkey(option)
	if err != nil {
		b.Fatalf("new hot key failed,err:=%v", err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h.Fading()
		}
	})
}

func BenchmarkHotkeyAutoCacheWrite100_100(b *testing.B) {
	benchmarkHotkey(b, true, 1)
}

func BenchmarkHotkeyWhilelist1Write10_100(b *testing.B) {
	var cacheRules []*CacheRuleConfig
	cacheRules = append(cacheRules, &CacheRuleConfig{Mode: "pattern", Value: "[0-9]{1,3}", TTL: 100*time.Millisecond})
	benchmarkHotkey(b, false, 0.1, cacheRules...)
}

func BenchmarkHotkeyWhilelist5Write10_100(b *testing.B) {
	var cacheRules []*CacheRuleConfig
	cacheRules = append(cacheRules, &CacheRuleConfig{Mode: "pattern", Value: "[0-1]{1,3}", TTL: 100*time.Millisecond})
	cacheRules = append(cacheRules, &CacheRuleConfig{Mode: "pattern", Value: "[2-3]{1,3}", TTL: 100*time.Millisecond})
	cacheRules = append(cacheRules, &CacheRuleConfig{Mode: "pattern", Value: "[4-5]{1,3}", TTL: 100*time.Millisecond})
	cacheRules = append(cacheRules, &CacheRuleConfig{Mode: "pattern", Value: ".*", TTL: 100*time.Millisecond})
	benchmarkHotkey(b, false, 0.1, cacheRules...)
}

func TestOnlyWhileList(t *testing.T) {
	var cacheRules []*CacheRuleConfig
	cacheRules = append(cacheRules, &CacheRuleConfig{Mode: "pattern", Value: "^1[0-9]{2}", TTL: 100*time.Millisecond})
	option := &Option{
		LocalCacheCap: 100,
		AutoCache:     false,
		TTL:           100 * time.Millisecond,
		WhileList:     cacheRules,
	}

	h, err := NewHotkey(option)
	if err != nil {
		t.Fatalf("new hot key failed,err:=%v", err)
	}
	for i := 0; i < 100; i++ {
		key := strconv.FormatInt(int64(i), 10)
		h.AddWithValue(key, key, 1)
		val := h.Get(key)
		assert.Nil(t, val, key)
	}
	for i := 100; i < 200; i++ {
		key := strconv.FormatInt(int64(i), 10)
		h.AddWithValue(key, key, 1)
		val := h.Get(key)
		assert.NotNil(t, val, key)
	}
	hots := h.List()
	assert.Equal(t, 0, len(hots))
}

func TestHotkeyWhilelist(t *testing.T) {
	var cacheRules []*CacheRuleConfig
	cacheRules = append(cacheRules, &CacheRuleConfig{Mode: "pattern", Value: "^1[0-9]{1,2}", TTL: 100*time.Millisecond})
	option := &Option{
		HotKeyCnt:     100,
		LocalCacheCap: 100,
		AutoCache:     false,
		TTL:           100 * time.Millisecond,
		WhileList:     cacheRules,
	}

	h, err := NewHotkey(option)
	if err != nil {
		t.Fatalf("new hot key failed,err:=%v", err)
	}
	for i := 100; i < 200; i++ {
		key := strconv.FormatInt(int64(i), 10)
		h.AddWithValue(key, key, 1)
		val := h.Get(key)
		assert.NotNil(t, val, key)
	}
	for i := 200; i < 300; i++ {
		key := strconv.FormatInt(int64(i), 10)
		h.AddWithValue(key, key, 1)
		val := h.Get(key)
		assert.Nil(t, val, key)
	}
}

func TestHotkeyBlacklist(t *testing.T) {
	var cacheRules []*CacheRuleConfig
	cacheRules = append(cacheRules, &CacheRuleConfig{Mode: "pattern", Value: "^2$", TTL: 100*time.Millisecond})
	cacheRules = append(cacheRules, &CacheRuleConfig{Mode: "pattern", Value: "^3$", TTL: 100*time.Millisecond})

	option := &Option{
		HotKeyCnt:     100,
		LocalCacheCap: 100,
		AutoCache:     true,
		TTL:           100 * time.Millisecond,
		BlackList:     cacheRules,
	}

	h, err := NewHotkey(option)
	if err != nil {
		t.Fatalf("new hot key failed,err:=%v", err)
	}
	zipf := rand.NewZipf(rand.New(rand.NewSource(uint64(time.Now().Unix()))), 2, 2, 1000)
	for i := 0; i < 100000; i++ {
		key := strconv.FormatInt(int64(zipf.Uint64()), 10)
		h.AddWithValue(key, key, 1)
	}
	for i := 0; i < 10; i++ {
		key := strconv.FormatInt(int64(i), 10)
		val := h.Get(key)
		if i == 2 || i == 3 {
			assert.Nil(t, val)
		} else {
			assert.NotNil(t, val)
		}
	}
}

func testHotkeyMinCount(t *testing.T) {
	option := &Option{
		HotKeyCnt:     10000,
		LocalCacheCap: 10000,
		AutoCache:     true,
		TTL:           1000 * time.Millisecond,
		MinCount:      10,
	}

	h, err := NewHotkey(option)
	if err != nil {
		t.Fatalf("new hot key failed,err:=%v", err)
	}
	for i := 0; i < 9; i++ {
		added := h.Add("1", 1)
		assert.False(t, added)
	}
	added := h.Add("1", 1)
	assert.True(t, added)
}

func testHotkeyHit(t *testing.T) {
	option := &Option{
		HotKeyCnt:     10000,
		LocalCacheCap: 10000,
		AutoCache:     true,
		TTL:           1000 * time.Millisecond,
	}

	h, err := NewHotkey(option)
	if err != nil {
		t.Fatalf("new hot key failed,err:=%v", err)
	}
	random := rand.New(rand.NewSource(uint64(time.Now().Unix())))
	zipf := rand.NewZipf(rand.New(rand.NewSource(uint64(time.Now().Unix()))), 1.1, 2, 10000000)
	var total int
	var hit int
	for {
		var i uint64 = zipf.Uint64()
		if total > 10000000 {
			i += uint64(total / 1000000)
		}
		key := strconv.FormatUint(i, 10)
		if random.Float64() < 0.50 {
			h.AddWithValue(key, key, 1)
		} else {
			total++
			val := h.Get(key)
			if val != nil {
				hit++
			}
		}
		if total%1000000 == 0 {
			fmt.Printf("hit ratio %v\n", float64(hit)/float64(total))
		}
	}
}

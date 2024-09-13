package engine

import (
	"net/netip"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestDuckdbTimeScan(t *testing.T) {
	var dt duckdbTime

	// Test case for pgtype.Time
	pgTime := pgtype.Time{Microseconds: 3600000000} // 1 hour in microseconds
	err := dt.Scan(pgTime)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	expectedTime := time.Now().Truncate(24 * time.Hour).Add(time.Hour).String()
	if string(dt) != expectedTime {
		t.Errorf("expected %s, got %s", expectedTime, dt)
	}

	// Test case for nil
	err = dt.Scan(nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if dt != "" {
		t.Errorf("expected empty string, got %s", dt)
	}

	// Test case for invalid type
	err = dt.Scan(123)
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestDuckdbTimeValue(t *testing.T) {
	dt := duckdbTime("test_time")
	val, err := dt.Value()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if val != "test_time" {
		t.Errorf("expected test_time, got %v", val)
	}
}

func TestDuckdbDurationScan(t *testing.T) {
	var dd duckdbDuration

	// Test case for pgtype.Interval
	pgInterval := pgtype.Interval{Microseconds: 1000000, Days: 1, Months: 1}
	err := dd.Scan(pgInterval)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	expectedDuration := "Microseconds: 1000000, Days: 1, Months: 1"
	if string(dd) != expectedDuration {
		t.Errorf("expected %s, got %s", expectedDuration, dd)
	}

	// Test case for nil
	err = dd.Scan(nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if dd != "" {
		t.Errorf("expected empty string, got %s", dd)
	}

	// Test case for invalid type
	err = dd.Scan(123)
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestDuckdbDurationValue(t *testing.T) {
	dd := duckdbDuration("test_duration")
	val, err := dd.Value()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if val != "test_duration" {
		t.Errorf("expected test_duration, got %v", val)
	}
}

func TestDuckdbNetIpPrefixScan(t *testing.T) {
	var dip duckdbNetIpPrefix

	// Test case for netip.Prefix
	prefix, _ := netip.ParsePrefix("192.168.1.0/24")
	err := dip.Scan(prefix)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	expectedPrefix := "192.168.1.0/24"
	if string(dip) != expectedPrefix {
		t.Errorf("expected %s, got %s", expectedPrefix, dip)
	}

	// Test case for nil
	err = dip.Scan(nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if dip != "" {
		t.Errorf("expected empty string, got %s", dip)
	}

	// Test case for invalid type
	err = dip.Scan(123)
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestDuckdbNetIpPrefixValue(t *testing.T) {
	dip := duckdbNetIpPrefix("test_prefix")
	val, err := dip.Value()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if val != "test_prefix" {
		t.Errorf("expected test_prefix, got %v", val)
	}
}

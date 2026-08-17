package main

import (
	"testing"
	"time"
)

// The limiter governs writes into a live Firestore project during a rollback
// replay. Getting it wrong is not a performance bug: too fast earns
// RESOURCE_EXHAUSTED partway through a replay someone is running precisely
// because something already went wrong.

func TestLimiterDisabledWhenRateIsZero(t *testing.T) {
	l := newLimiter(0)
	start := time.Now()
	for range 10 {
		l.wait(1000)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("rate 0 must not block, slept %v", elapsed)
	}
}

func TestLimiterAllowsBudgetWithoutSleeping(t *testing.T) {
	l := newLimiter(500)
	start := time.Now()
	l.wait(200)
	l.wait(200) // 400 total, still inside the 500/s budget
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("writes inside the budget must not sleep, slept %v", elapsed)
	}
	if l.spent != 400 {
		t.Errorf("spent = %d, want 400", l.spent)
	}
}

func TestLimiterSleepsWhenBudgetWouldOverrun(t *testing.T) {
	l := newLimiter(100)
	start := time.Now()
	l.wait(80)
	l.wait(80) // would reach 160 in one second — must wait out the window
	elapsed := time.Since(start)
	if elapsed < 900*time.Millisecond {
		t.Errorf("overrunning the budget should wait out the window, slept only %v", elapsed)
	}
	// After sleeping it starts a fresh window rather than carrying the debt.
	if l.spent != 80 {
		t.Errorf("spent after window reset = %d, want 80", l.spent)
	}
}

func TestLimiterResetsAfterAQuietSecond(t *testing.T) {
	l := newLimiter(100)
	l.wait(100) // budget fully spent
	time.Sleep(1100 * time.Millisecond)

	start := time.Now()
	l.wait(100) // a new window: should be free
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("a fresh window must not sleep, slept %v", elapsed)
	}
}

func TestLimiterHandlesBatchLargerThanBudget(t *testing.T) {
	// A 500-document batch against a 400/s limit can never fit. It must still
	// make progress rather than spin forever.
	l := newLimiter(400)
	done := make(chan struct{})
	go func() { l.wait(500); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a batch larger than the per-second budget must still proceed")
	}
}

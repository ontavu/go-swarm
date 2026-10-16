// Copyright (c) 2022-2024 The authors (see the AUTHORS file)
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package swarm

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// waitCount polls Count() until it reaches want, and reports what it last saw.
// Polling is necessary because a member is only removed from the count once its
// goroutine has actually returned.
func waitCount(t *testing.T, s Swarm, want uint32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := s.Count()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Count() = %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// groupMakers covers both implementations, which share realSwarm.Run and so
// share the counter under test.
var groupMakers = []struct {
	name string
	make func(context.Context) Swarm
}{
	{"Swarm", New},
	{"Ensemble", NewEnsemble},
}

func TestSwarm_CountEmpty(t *testing.T) {
	for _, tc := range groupMakers {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.make(context.Background())
			defer s.Cancel()
			if got := s.Count(); got != 0 {
				t.Fatalf("fresh group: Count() = %d, want 0", got)
			}
		})
	}
}

// TestSwarm_CountReturnsToZero is the regression test for minusOne: with
// MaxUint32-1 the decrement subtracted 2 and the counter underflowed to
// 4294967294 as soon as the first member returned.
func TestSwarm_CountReturnsToZero(t *testing.T) {
	for _, tc := range groupMakers {
		t.Run(tc.name, func(t *testing.T) {
			for _, members := range []int{1, 2, 5} {
				s := tc.make(context.Background())
				for i := 0; i < members; i++ {
					s.Run(func(context.Context) {})
				}
				s.Wait()
				if got := s.Count(); got != 0 {
					t.Fatalf("%d members: Count() = %d after Wait(), want 0", members, got)
				}
				s.Cancel()
			}
		})
	}
}

// TestSwarm_CountCountsLiveMembers checks the counter while the members are
// still running, and that Run() accounts for a member before returning.
func TestSwarm_CountCountsLiveMembers(t *testing.T) {
	for _, tc := range groupMakers {
		t.Run(tc.name, func(t *testing.T) {
			const members = 4

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := tc.make(ctx)
			defer s.Cancel()

			started := make(chan struct{}, members)
			for i := 0; i < members; i++ {
				s.Run(func(c context.Context) {
					started <- struct{}{}
					<-c.Done()
				})
				// Run() increments before spawning, so the member is already
				// accounted for even if it has not been scheduled yet.
				if got := s.Count(); got < uint32(i+1) {
					t.Fatalf("after Run() #%d: Count() = %d, want >= %d", i+1, got, i+1)
				}
			}

			for i := 0; i < members; i++ {
				<-started
			}
			if got := s.Count(); got != members {
				t.Fatalf("all members blocked: Count() = %d, want %d", got, members)
			}

			s.Cancel()
			s.Wait()
			waitCount(t, s, 0)
		})
	}
}

// TestSwarm_MembersObserveCancel checks that Cancel() releases every member and
// that Wait() then returns.
func TestSwarm_MembersObserveCancel(t *testing.T) {
	for _, tc := range groupMakers {
		t.Run(tc.name, func(t *testing.T) {
			const members = 3

			s := tc.make(context.Background())
			var done atomic.Uint32
			for i := 0; i < members; i++ {
				s.Run(func(c context.Context) {
					<-c.Done()
					done.Add(1)
				})
			}

			s.Cancel()
			s.Wait()

			if got := done.Load(); got != members {
				t.Fatalf("%d members observed the cancellation, want %d", got, members)
			}
			if got := s.Count(); got != 0 {
				t.Fatalf("Count() = %d after Wait(), want 0", got)
			}
		})
	}
}

// TestSwarm_ParentCancelPropagates checks that cancelling the parent context
// releases the members, since that is how the agent shuts its groups down.
func TestSwarm_ParentCancelPropagates(t *testing.T) {
	for _, tc := range groupMakers {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			s := tc.make(ctx)
			defer s.Cancel()

			s.Run(func(c context.Context) { <-c.Done() })
			cancel()
			s.Wait()

			if got := s.Count(); got != 0 {
				t.Fatalf("Count() = %d after Wait(), want 0", got)
			}
		})
	}
}

// TestSwarm_OneMemberDoesNotCancelOthers pins the difference between the two
// implementations: in a plain Swarm the members have independent lifetimes.
func TestSwarm_OneMemberDoesNotCancelOthers(t *testing.T) {
	s := New(context.Background())
	defer s.Cancel()

	cancelled := make(chan struct{})
	s.Run(func(c context.Context) {
		<-c.Done()
		close(cancelled)
	})
	s.Run(func(context.Context) {}) // returns immediately

	waitCount(t, s, 1)
	select {
	case <-cancelled:
		t.Fatal("a returning member cancelled the rest of the Swarm")
	case <-time.After(50 * time.Millisecond):
	}

	s.Cancel()
	s.Wait()
	waitCount(t, s, 0)
}

// TestEnsemble_OneMemberCancelsOthers pins the opposite guarantee: an Ensemble
// is all-or-nothing, so the exit of any member tears the group down.
func TestEnsemble_OneMemberCancelsOthers(t *testing.T) {
	s := NewEnsemble(context.Background())
	defer s.Cancel()

	var cancelled atomic.Bool
	s.Run(func(c context.Context) {
		<-c.Done()
		cancelled.Store(true)
	})
	s.Run(func(context.Context) {}) // returns immediately, must cancel the group

	s.Wait()

	if !cancelled.Load() {
		t.Fatal("a returning member did not cancel the rest of the Ensemble")
	}
	if got := s.Count(); got != 0 {
		t.Fatalf("Count() = %d after Wait(), want 0", got)
	}
}

// TestRun_WaitsForCallbacks covers the Run/EnsembleRun helpers, which
// are what main() and the agent actually call.
func TestRun_WaitsForCallbacks(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(context.Context, ...Func)
	}{
		{"Run", Run},
		{"EnsembleRun", EnsembleRun},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var done atomic.Uint32
			tc.run(context.Background(),
				func(context.Context) { done.Add(1) },
				func(context.Context) { done.Add(1) },
			)
			if got := done.Load(); got != 2 {
				t.Fatalf("%d callbacks completed before return, want 2", got)
			}
		})
	}
}

// TestRun_WaitsForEveryMember pins Run's actual contract: it returns when
// every callback has returned. Dropping its Wait breaks this; swapping its two
// defers does not, since these members ignore their context, and is caught by
// the next test.
func TestRun_WaitsForEveryMember(t *testing.T) {
	var finished atomic.Uint32

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(context.Background(),
			func(context.Context) { time.Sleep(20 * time.Millisecond); finished.Add(1) },
			func(context.Context) { time.Sleep(40 * time.Millisecond); finished.Add(1) },
		)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run never returned")
	}

	if got := finished.Load(); got != 2 {
		t.Fatalf("%d members finished before Run returned, want 2", got)
	}
}

// TestRun_HonoursTheCallersContext is the other half: a member that waits
// on its context is released by the caller, not by Run.
func TestRun_HonoursTheCallersContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, func(c context.Context) { <-c.Done() })
	}()

	select {
	case <-done:
		t.Fatal("Run returned before its caller cancelled: it cancelled its own members")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
}

// TestEnsembleRun_ReturnsWhenAMemberDoes is the difference between the two
// groups, and the reason a server loop belongs in an Ensemble: one member
// returning releases the others.
func TestEnsembleRun_ReturnsWhenAMemberDoes(t *testing.T) {
	released := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		EnsembleRun(context.Background(),
			// Stands in for a server loop that has stopped on its own.
			func(context.Context) {},
			// Stands in for a watchdog waiting to be told to shut down.
			func(c context.Context) { <-c.Done(); close(released) },
		)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("EnsembleRun did not return when one member did")
	}
	select {
	case <-released:
	default:
		t.Fatal("the other member was not released")
	}
}

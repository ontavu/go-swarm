// Copyright (c) 2022-2024 The authors (see the AUTHORS file)
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Package swarm supervises groups of goroutines that share a cancel and are
// waited on together.
//
// The members of a Swarm, made by New, stop only when the group is cancelled.
// The members of an Ensemble, made by NewEnsemble, stop as soon as any one of
// them returns.
package swarm

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
)

type Func func(context.Context)

type Swarm interface {
	Cancel()
	Wait()
	Run(cb Func)
	Count() uint32
}

func Run(ctx0 context.Context, callbacks ...Func) {
	s := New(ctx0)
	// The order is deliberate and easy to get wrong in both directions.
	// Deferred calls run last-in first-out, so this is Wait and then Cancel:
	// Run's contract is to return once every callback has returned, and the
	// Cancel afterwards releases the derived context rather than releasing the
	// members. Swapping them makes Run cancel the work it has just started.
	//
	// A member that never returns therefore blocks here for good. That is the
	// contract, not a defect: a member is responsible for observing its own
	// context. A group whose members should stop when any one of them does is
	// an Ensemble, not a Swarm.
	defer s.Cancel()
	defer s.Wait()
	for _, cb := range callbacks {
		s.Run(cb)
	}
}

type realSwarm struct {
	wg     sync.WaitGroup
	cancel context.CancelFunc
	ctx    context.Context
	active uint32
}

func newRealSwarm(ctx context.Context) *realSwarm {
	ctx2, cancel := context.WithCancel(ctx)
	return &realSwarm{
		wg:     sync.WaitGroup{},
		ctx:    ctx2,
		cancel: cancel,
	}
}

func New(ctx context.Context) Swarm { return newRealSwarm(ctx) }

func (s *realSwarm) Cancel() { s.cancel() }

func (s *realSwarm) Wait() { s.wg.Wait() }

func (s *realSwarm) Count() uint32 { return atomic.LoadUint32(&s.active) }

const (
	// minusOne is the two's complement encoding of -1 for a uint32, meant to be
	// passed to atomic.AddUint32 as a decrement. It has to be exactly MaxUint32:
	// MaxUint32-1 subtracts 2 and makes the counter underflow.
	minusOne uint32 = math.MaxUint32
)

func (s *realSwarm) Run(cb Func) {
	// Account for the new member before it starts, so that Count() never reports
	// an idle group in the window between Run() returning and the goroutine
	// being scheduled.
	s.wg.Add(1)
	atomic.AddUint32(&s.active, 1)
	go func() {
		defer s.wg.Done()
		defer atomic.AddUint32(&s.active, minusOne)
		cb(s.ctx)
	}()
}

func EnsembleRun(ctx0 context.Context, callbacks ...Func) {
	s := NewEnsemble(ctx0)
	// Wait then Cancel, as in Run. Here the wait is bounded by the group
	// itself: an Ensemble cancels every member as soon as one of them returns.
	defer s.Cancel()
	defer s.Wait()
	for _, cb := range callbacks {
		s.Run(cb)
	}
}

type realEnsemble struct {
	swarm realSwarm
}

func NewEnsemble(ctx context.Context) Swarm { return &realEnsemble{*newRealSwarm(ctx)} }

func (s *realEnsemble) Cancel() { s.swarm.Cancel() }

func (s *realEnsemble) Wait() { s.swarm.Wait() }

func (s *realEnsemble) Count() uint32 { return s.swarm.Count() }

func (s *realEnsemble) Run(cb Func) {
	s.swarm.Run(func(ctx context.Context) {
		// Whatever the exit cause of the cb, this cancellation triggers the
		// exit of all the other cb of the Group
		defer s.Cancel()
		cb(ctx)
	})
}

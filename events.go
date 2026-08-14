package tlndb

import (
	"context"
	"sync"
)

// EventKind classifies a MutationEvent. Mirrors tln-language's
// factstore.EventKind so the adapter can map straight across.
type EventKind int

const (
	// EventAssert fires after a brand-new document is committed.
	EventAssert EventKind = iota
	// EventChange fires when an existing document's bytes change.
	// OldDoc carries the prior content; NewDoc carries the committed one.
	EventChange
	// EventRetract fires after a Delete commits. OldDoc carries the
	// content that was just removed.
	EventRetract
)

func (k EventKind) String() string {
	switch k {
	case EventAssert:
		return "assert"
	case EventChange:
		return "change"
	case EventRetract:
		return "retract"
	}
	return "unknown"
}

// MutationEvent is what subscribers receive. Field population:
//
//	assert  : NewDoc set, OldDoc empty
//	change  : both NewDoc and OldDoc set
//	retract : OldDoc set, NewDoc empty
type MutationEvent struct {
	Kind        EventKind
	EntityID    string
	DocID       string
	OldDoc      []byte
	NewDoc      []byte
	AtUnixNanos int64
}

// EventSubscriber receives mutation events. Subscribers run
// synchronously in the emitter's goroutine; if a subscriber needs to
// do significant work, it should hand the event off to a channel and
// return quickly.
type EventSubscriber func(ctx context.Context, ev MutationEvent)

// EventEmitter is a tiny fan-out helper. Stores embed (or compose)
// one to gain Subscribe / Emit. Subscribers are fired in registration
// order; the emitter releases its read lock before invoking them so
// subscribers may re-enter the store without deadlock.
type EventEmitter struct {
	mu   sync.RWMutex
	subs []EventSubscriber
}

// Subscribe registers a subscriber. The returned function
// unsubscribes; calling it again is a no-op.
func (e *EventEmitter) Subscribe(s EventSubscriber) (unsubscribe func()) {
	e.mu.Lock()
	idx := len(e.subs)
	e.subs = append(e.subs, s)
	e.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			e.mu.Lock()
			if idx < len(e.subs) {
				e.subs[idx] = nil
			}
			e.mu.Unlock()
		})
	}
}

// Emit fans an event out to every live subscriber, in registration
// order. Exported so store implementations in other packages can call
// it after a successful commit.
func (e *EventEmitter) Emit(ctx context.Context, ev MutationEvent) {
	e.mu.RLock()
	snapshot := make([]EventSubscriber, len(e.subs))
	copy(snapshot, e.subs)
	e.mu.RUnlock()
	for _, s := range snapshot {
		if s != nil {
			s(ctx, ev)
		}
	}
}

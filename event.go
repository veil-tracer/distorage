package distorage

// Event represents an event type
type Event uint8

// EventListener represents a function type for event listeners
type EventListener func(e Event, data ...any)

const (
	EventReBalance Event = 1 << iota
	EventMissedLookup
	EventFullRingLookup
)

func (ch *ConsistentHash) trigger(e Event, data ...any) {
	if ch.listeners == nil {
		return
	}
	for _, listener := range ch.listeners[e] {
		listener(e, data...)
	}
}

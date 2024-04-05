package distorage

// Event type represents different events that can be triggered
type Event uint8

// EventListener is a function type that listens for specific events
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

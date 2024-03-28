package distorage

type Event int

type EventListener func(e Event, data ...any)

const (
	EventReBalance Event = 1 << iota
	EventReBalanceShift
	EventMissedLookupBlock
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

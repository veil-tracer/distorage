package distorage

type Event uint8

type EventListener func(e Event, data ...any)

const (
	EventReBalance Event = 1 << iota
	EventMissedLookup
	EventFullRingLookup
)

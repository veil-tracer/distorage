package distorage

type Event uint8

type EventListener func(e Event, data ...any)

const (
	EventReBalance Event = 1 << iota
	EventMissedLookup
	EventFullRingLookup
)

// Removed trigger function as it is marked unused and no implementation details specified for future use.


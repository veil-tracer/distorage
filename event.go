package distorage

// Event represents the type of event that can be triggered within distorage.
type Event uint8

// EventListener defines the function signature for listeners that can react to Events.
type EventListener func(e Event, data ...any)

// EventReBalance is triggered when the distribution of keys needs to be rebalanced.
// EventMissedLookup is triggered when a key lookup fails.
// EventFullRingLookup is triggered when a full ring scan is performed.
const (
	EventReBalance Event = 1 << iota
	EventMissedLookup
	EventFullRingLookup
)

// trigger notifies all registered listeners of the given event.
func (ch *ConsistentHash) trigger(e Event, data ...any) {
	if ch.listeners == nil {
		return
	}
	for _, listener := range ch.listeners[e] {
		listener(e, data...)
	}
}

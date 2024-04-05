package distorage

// Option is a type that defines a function that configures an options instance.
// It is used to apply configuration options to the distorage system.
type Option func(*options)

type options struct {
	hashFunc          HashFunc
	listeners         map[Event][]EventListener
	defaultReplicas   uint
	blockPartitioning int
}

// WithDefaultReplicas sets the default number of replicas to add for each key.
func WithDefaultReplicas(replicas uint) Option {
	return func(o *options) {
		o.defaultReplicas = replicas
	}
}

// WithHashFunc sets the hash function for 32bit consistent hashing.
func WithHashFunc(hashFunc HashFunc) Option {
	return func(o *options) {
		o.hashFunc = hashFunc
	}
}

// WithListener attaches a listener to the specified event types.
func WithListener(listener EventListener, e ...Event) Option {
	return func(o *options) {
		if o.listeners == nil {
			o.listeners = make(map[Event][]EventListener, 1)
		}
		for _, event := range e {
			o.listeners[event] = append(o.listeners[event], listener)
		}
	}
}

// WithBlockPartitioning sets the block partitioning mode, dividing the total
// number of keys by the given number to determine the number of blocks.
func WithBlockPartitioning(divisionBy int) Option {
	return func(o *options) {
		o.blockPartitioning = divisionBy
	}
}

package distorage

// options struct holds all the configurable options for the distributed storage
type options struct {
	hashFunc          HashFunc
	listeners         map[Event][]EventListener
	defaultReplicas   uint
	blockPartitioning int
}

type Option func(*options)

// WithDefaultReplicas sets the default number of replicas to add for each key
func WithDefaultReplicas(replicas uint) Option {
	return func(o *options) {
		o.defaultReplicas = replicas
	}
}

// WithHashFunc sets the hash function for consistent hashing
func WithHashFunc(hashFunc HashFunc) Option {
	return func(o *options) {
		o.hashFunc = hashFunc
	}
}

// WithListener attaches an event listener to the specified event(s)
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

// WithBlockPartitioning enables block partitioning by dividing keys into specified number of blocks
func WithBlockPartitioning(divisionBy int) Option {
	return func(o *options) {
		o.blockPartitioning = divisionBy
	}
}

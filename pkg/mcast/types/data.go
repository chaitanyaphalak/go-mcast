package types

// Used to transfer information.
type DataHolder struct {
	// What kind of operation is being executed.
	Operation Operation

	// This will be used to associate the value with something
	// so the retrieval can be done more easily.
	// If nothing is provided, will be generated a value to
	// be used based on the cluster information.
	Key []byte

	// If there is any value to be written into the
	// state machine it will be hold here.
	// This will only have a value if the operation is
	// of type command.
	// If the value is nil, then the state machine value
	// associated will also be nil.
	Content []byte

	// Extensions holds an opaque byte slice of information for middleware. It
	// is up to the client of the library to properly modify this as it adds
	// layers and remove those layers when appropriate.
	// The entry will hold the same extension sent by the used.
	Extensions []byte
}

// Entry on the log abstraction. This entry hold the byte information
// that is transported, a serialized Message object.
// Also holds which kind of Operation was applied and if the Message
// was generic delivered.
type LogEntry struct {
	// A serialized Message struct. This is hold as a byte slice to
	// enable future log compression.
	Data []byte

	// Which kind of operation was applied that generated this entry.
	Operation Operation

	// A boolean to indicate if this entry was generated by the
	// generic delivery of the protocol.
	GenericDelivered bool
}

// Entry object applied to the Storage interface.
// This entry holds information about the data transferred,
// the unique identifier generate by the protocol.
type StorageEntry struct {
	// The UID generated by the protocol. This will be passed and
	// is up to the client to do anything with it.
	Key UID

	// The transferred data, this is the content sent to the protocol
	// for replication.
	Value DataHolder
}
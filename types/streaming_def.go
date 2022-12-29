package types

// StreamConnectMessage describes a message used in stream joining.
// Users send this message when they want to join particular stream
// - implements types.Message
type StreamConnectMessage struct {
	// StreamID is an id of stream for joining
	StreamID string

	// ClientID is the client
	ClientID string

	// StreamerID is the address of the streamer
	StreamerID string
}

// StreamDisconnectMessage describes a message used in stream disconnecting.
// Users send this message when they want to disconnect from particular stream
// - implements types.Message
type StreamDisconnectMessage struct {
	// StreamID is an id of stream for disconnecting
	StreamID string

	// ClientID is the client
	ClientID string

	// StreamerID is the address of the streamer
	StreamerID string
}

// StreamStartMessage describes a message used to notify the start of the new stream.
// Server send this message when they want to notify users about the start of the stream
// - implements types.Message
type StreamStartMessage struct {
	// StreamID is an id of stream for disconnecting
	StreamID string

	// streamInfo contains all information about the stream
	StreamInfo StreamInfo
	// StreamerID is the address of the streamer
	StreamerID string
}

// StreamStopMessage describes a message used to notify the end of the stream.
// Server send this message when they want to notify users about the end of the stream
// - implements types.Message
type StreamStopMessage struct {
	// StreamID is an id of stream for disconnecting
	StreamID   string
	StreamerID string
}

type StreamAcceptMessage struct {
	// StreamID is an id of stream for disconnecting
	StreamID string

	// ClientID is the address of the client
	ClientID string

	// StreamerID is the address of the streamer
	StreamerID string

	// Symmetric key encrypted with PK of client
	EncSymmetricKey []byte

	// Flag if the client is accepted
	Accepted bool
}

// StreamMessage contains info about the stream
type StreamMessage struct {
	// StreamInfo contains all information about the stream
	StreamInfo StreamInfo

	// Data contains the chunk of a stream
	Data StreamData
}

// StreamDataMessage contains encrypted stream data
type StreamDataMessage struct {
	// ID contains streamID
	ID string

	// Payload contains encrypted payload
	Payload string
}

// StreamInfo contains additional information about the stream
type StreamInfo struct {

	// StreamID is the id of the stream
	StreamID string

	// Name is the name of the movie
	Name string

	// Price is the amount required to pay in order to participate in the stream
	Price uint

	// Grade presents the average grade so far for this stream
	Grade float64

	// CurrentlyWatching presents the number of users currently watching the strea,
	CurrentlyWatching uint
}

type StreamData struct {
	Chunk      []byte
	StartIndex uint
	EndIndex   uint
}

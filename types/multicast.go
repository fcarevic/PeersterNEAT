package types

import "go.dedis.ch/cs438/transport"

// MulticastMessage contains info about the stream
type MulticastMessage struct {
	// ID is the ID of the stream
	ID string

	// Message
	Message *transport.Message
}

// MulticastStopMessage contains info about the stream
type MulticastStopMessage struct {
	// ID is the ID of the stream
	ID string

	// Message
	Message *transport.Message
}

// MulticastJoinMessage contains info about the stream
type MulticastJoinMessage struct {
	// ID is the ID of the stream
	ID string

	// ClientID address of client
	ClientID string

	// Address of the streamer
	StreamerID string

	// Message
	Message *transport.Message
}

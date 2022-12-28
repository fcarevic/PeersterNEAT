package peer

import (
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// Streaming describes functions used in multicast streaming
type Multicast interface {

	// Multicast multicasts the message
	Multicast(msg types.MulticastMessage) error

	// GetMulticastClients Returns the clients for the chosen stream
	GetMulticastClients(streamID string) ([]string, error)

	// JoinMulticast Join the stream with corresponding ID. Returns error if the stream does not exist or the join
	// was unsuccessful. (msg contains info for higher-level protocol)
	JoinMulticast(streamID string, streamerID string, msg *transport.Message) error

	// StopMulticast announce that the stream has ended.
	StopMulticast(streamID string, message transport.Message) error

	// StreamFFMPG4 streams video clips
	StreamFFMPG4(manifestName string, dir string, name string, price uint, streamID string)

	// ReceiveFFMPG4 receives and decodes video clips
	ReceiveFFMPG4(streamID string, dir string) error
}

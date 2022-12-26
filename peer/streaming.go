package peer

import (
	"go.dedis.ch/cs438/types"
	"io"
)

const STREAMINGSIZE = 65536

// Streaming describes functions used in multicast streaming
type Streaming interface {
	// StartStreaming starts streaming of provided file, returns the ID of stream.
	AnnounceStreaming(name string, price uint) (stringID string, err error)

	// StartStreaming starts streaming of provided file, returns the ID of stream.
	Stream(data io.Reader, name string, price uint, streamID string) (err error)

	// Stop the stream
	//StopStreaming(streamID string) error

	// GetClients Returns the clients for the chosen stream
	GetClients(streamID string) ([]string, error)

	// ConnectToStream These operations are for the client nodes
	// ConnectToStream Join the stream with corresponding ID. Returns error if the stream does not exist or the join was unsuccessful.
	ConnectToStream(streamID string, streamerID string) error

	// Disconnect from stream, returns error if not connected or the stream does not exist
	//DisconnectFromStream(streamID string) error

	// GetNextChunks returns numberOfChunks last received chunks for the streamID. returns error if the chunk does not exist.
	GetNextChunks(streamID string, numberOfChunks int) ([]types.StreamMessage, error)

	StreamFFMPG4(manifestName string, dir string, name string, price uint, streamID string)
	ReceiveFFMPG4(streamID string, dir string) error
}

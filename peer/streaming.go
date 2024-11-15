package peer

import (
	"go.dedis.ch/cs438/types"
	"io"
)

// Streaming describes functions used in multicast streaming
type Streaming interface {
	// AnnounceStartStreaming starts streaming of provided file, returns the ID of stream.
	AnnounceStartStreaming(name string, price uint, thumbnail []byte) (stringID string, err error)

	// StartStreaming starts streaming of provided file, returns the ID of stream.
	Stream(data io.Reader, name string, price uint, streamID string, thumbnail []byte, seqNum uint) (err error)

	// AnnounceStopStreaming stops the stream, returns and error if the stream does not exist
	AnnounceStopStreaming(streamID string) error

	// GetClients Returns the clients for the chosen stream
	GetClients(streamID string) ([]string, error)

	// RemoveStreamClient removes the client
	RemoveStreamClient(streamID string, clientID string) error

	// ConnectToStream Join the stream with corresponding ID. Returns error if the
	//stream does not exist or the join was unsuccessful.
	ConnectToStream(streamID string, streamerID string) error

	// ReactToStream send reaction for streamID
	ReactToStream(streamID string, streamerID string, grade float64) error

	// GetNextChunks returns numberOfChunks last received chunks for the streamID.
	//returns error if the chunk does not exist.
	GetNextChunks(streamID string, numberOfChunks int) ([]types.StreamMessage, error)

	// GetAllStreams returns all available streams in the network
	GetAllStreams() []types.StreamInfo
}

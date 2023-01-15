package peer

const STREAMINGSIZE = 65536

// Streaming describes functions used in multicast streaming
type Hls interface {
	// StreamFFMPG4 streams video clips
	StreamFFMPG4(manifestName string, dir string, name string, price uint, streamID string, thumbnail []byte)

	// ReceiveFFMPG4 receives and decodes video clips
	ReceiveFFMPG4(streamID string, dir string) error
}

package types

import "go.dedis.ch/cs438/transport"

// CrowdsMessage contains a bag of recipients that form the crowds cluster
// as well as the msg to be sent in the cluster.
// CrowdsMessage needs to be encrypted before sending.
type CrowdsMessage struct {
	// Recipients is a list of recipients that form crowds cluster.
	Recipients []string

	// Msg is either CrowdsMessagingRequestMessage or CrowdsDownloadRequestMessage.
	Msg *transport.Message
}

// CrowdsMessagingRequestMessage contains message that is meant for final destination.
// Embedded message can be ChatMessage or ConfidentMessage.
type CrowdsMessagingRequestMessage struct {
	// FinalDst is the address of the node that should receive the Msg.
	FinalDst string
	// Msg to be delivered to final destination, either ChatMessage or ConfidMessage.
	Msg *transport.Message
}

// CrowdsDownloadRequestMessage contains metahash of file that src wants to download.
type CrowdsDownloadRequestMessage struct {
	// Origin is the address of the node that requested the download.
	Origin string

	// RequestID must be a unique identifier. Use xid.New().String() to generate
	// it.
	RequestID string

	// Key can be either a hex-encoded metahash or chunk hash.
	Key string
}

// CrowdsDownloadReplyMessage contains metahash of file that src wants to download.
type CrowdsDownloadReplyMessage struct {
	// RequestID must be the same as the RequestID set in the
	// CrowdsDownloadRequestMessage.
	RequestID string

	// Key must be the same as the Key set in the CrowdsDownloadRequestMessage.
	Key string

	// Index of chunk.
	Index int

	// TotalChunks is total num of chunks.
	TotalChunks int

	// Chunk value.
	Value []byte

	// Chunk metahash.
	Metahash string
}

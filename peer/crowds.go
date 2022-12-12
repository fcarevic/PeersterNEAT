package peer

import "go.dedis.ch/cs438/transport"

// Crowds defines the functions for crowds messaging
type Crowds interface {
	CrowdsInit(conf Configuration)
	CrowdsDestroy()
	StartCrowds(peers []string, isFileRequest bool, content, finalDst string) ([]byte, error)
	SendCrowdsMessage(embeddedMsg *transport.Message, recipients []string) error
	CreateCrowdsMessagingRequest(dst, content string) (transport.Message, error)
	CreateCrowdsDownloadRequest(reqID, content string) (transport.Message, error)
}

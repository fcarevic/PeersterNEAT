package peer

// Crowds defines the functions for crowds messaging
type Crowds interface {
	CrowdsSend(peers []string, body, to string) error
	CrowdsDownload(peers []string, filename string) ([]byte, error)
}

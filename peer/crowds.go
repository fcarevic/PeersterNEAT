package peer

// Crowds defines the functions for crowds messaging
type Crowds interface {
	// CrowdsSend runs Crowds for sending anonymous message.
	CrowdsSend(peers []string, body, to string) error

	// CrowdsDownload runs Crowds for anonymously downloading files.
	CrowdsDownload(peers []string, filename string) (bool, error)
}

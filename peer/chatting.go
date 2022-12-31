package peer

type ChatMessageInfo struct {
	Receiver string
	Message  string
}

// Crowds defines the functions for crowds messaging
type Chatting interface {
	GetChatMessages() []ChatMessageInfo
	AddChatMessage(msg string, to string)
}

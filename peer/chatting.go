package peer

type ChatMessageInfo struct {
	Sender   string
	Receiver string
	Message  string
}

// Crowds defines the functions for crowds messaging
type Chatting interface {
	GetSentChatMessages() []ChatMessageInfo
	GetReceivedChatMessages() []ChatMessageInfo
	RegisterSentMessage(msg string, to string)
	RegisterReceivedMessage(msg string, from string)
}

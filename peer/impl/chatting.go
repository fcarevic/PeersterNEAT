package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"sync"
)

type ChatInfo struct {
	receivedMessages []peer.ChatMessageInfo
	sentMessages     []peer.ChatMessageInfo
	chatInfoMutex    sync.Mutex
}

func (c *ChatInfo) GetSentChatMessages() []peer.ChatMessageInfo {
	c.chatInfoMutex.Lock()
	defer c.chatInfoMutex.Unlock()
	tmp := make([]peer.ChatMessageInfo, len(c.sentMessages))
	copy(tmp, c.sentMessages)
	return tmp
}

func (c *ChatInfo) GetReceivedChatMessages() []peer.ChatMessageInfo {
	c.chatInfoMutex.Lock()
	defer c.chatInfoMutex.Unlock()
	tmp := make([]peer.ChatMessageInfo, len(c.receivedMessages))
	copy(tmp, c.sentMessages)
	return tmp
}

func (c *ChatInfo) RegisterSentMessage(msg string, to string, from string) {
	chatMsg := peer.ChatMessageInfo{
		Message:  msg,
		Receiver: to,
		Sender:   from,
	}
	c.chatInfoMutex.Lock()
	defer c.chatInfoMutex.Unlock()
	c.sentMessages = append(c.sentMessages, chatMsg)
}
func (c *ChatInfo) RegisterReceivedMessage(msg string, to string, from string) {
	chatMsg := peer.ChatMessageInfo{
		Message:  msg,
		Receiver: to,
		Sender:   from,
	}
	c.chatInfoMutex.Lock()
	defer c.chatInfoMutex.Unlock()
	c.sentMessages = append(c.receivedMessages, chatMsg)
}

func (c *ChatInfo) GetSentChatMessages() []peer.ChatMessageInfo {
	c.chatInfoMutex.Lock()
	defer c.chatInfoMutex.Unlock()
	tmp := make([]peer.ChatMessageInfo, len(c.sentMessages))
	copy(tmp, c.sentMessages)
	return tmp
}

func (c *ChatInfo) GetReceivedChatMessages() []peer.ChatMessageInfo {
	c.chatInfoMutex.Lock()
	defer c.chatInfoMutex.Unlock()
	tmp := make([]peer.ChatMessageInfo, len(c.receivedMessages))
	copy(tmp, c.sentMessages)
	return tmp
}

func (c *ChatInfo) RegisterSentMessage(msg string, to string, from string) {
	chatMsg := peer.ChatMessageInfo{
		Message:  msg,
		Receiver: to,
		Sender:   from,
	}
	c.chatInfoMutex.Lock()
	defer c.chatInfoMutex.Unlock()
	c.sentMessages = append(c.sentMessages, chatMsg)
}
func (n *node) RegisterReceivedMessage(msg string, from string) {
	return n.chatInfo.RegisterReceivedMessage(msg, n.conf.Socket.GetAddress(), from)
}

func (n *node) RegisterSentMessage(msg string, to string) {
	return n.chatInfo.RegisterSentMessage(msg, to, n.conf.Socket.GetAddress())
}

func (n *node) GetReceivedChatMessages() []peer.ChatMessageInfo {
	return n.chatInfo.GetReceivedChatMessages()
}
func (n *node) GetSentChatMessages() []peer.ChatMessageInfo {
	return n.chatInfo.GetSentChatMessages()
}

// Callback function for chat message
func (n *node) chatMessageCallback(msg types.Message, pkt transport.Packet) error {

	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to Chat message got wrong type: %T", msg)
	}

	n.RegisterReceivedMessage(chatMsg.Message, pkt.Header.Source)
	// Log received message
	log.Info().Msgf(
		"Source: %s \t Destination: %s: \t MessageType: %s \t MessageBody: %s",
		pkt.Header.Source,
		pkt.Header.Destination,
		pkt.Msg.Type,
		chatMsg)
	return nil
}

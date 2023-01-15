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
	messages      []peer.ChatMessageInfo
	chatInfoMutex sync.Mutex
}

func (c *ChatInfo) GetMessages() []peer.ChatMessageInfo {
	c.chatInfoMutex.Lock()
	defer c.chatInfoMutex.Unlock()
	tmp := make([]peer.ChatMessageInfo, len(c.messages))
	copy(tmp, c.messages)
	return tmp
}

func (c *ChatInfo) AddMessage(msg string, to string) {
	chatMsg := peer.ChatMessageInfo{
		Message:  msg,
		Receiver: to,
	}
	c.chatInfoMutex.Lock()
	defer c.chatInfoMutex.Unlock()
	c.messages = append(c.messages, chatMsg)
}

func (n *node) AddChatMessage(msg string, to string) {
	n.chatInfo.AddMessage(msg, to)
}

func (n *node) GetChatMessages() []peer.ChatMessageInfo {
	return n.chatInfo.GetMessages()
}

// Callback function for chat message
func (n *node) chatMessageCallback(msg types.Message, pkt transport.Packet) error {

	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to Chat message got wrong type: %T", msg)
	}

	n.AddChatMessage(chatMsg.Message, pkt.Header.Destination)
	// Log received message
	log.Info().Msgf(
		"Source: %s \t Destination: %s: \t MessageType: %s \t MessageBody: %s",
		pkt.Header.Source,
		pkt.Header.Destination,
		pkt.Msg.Type,
		chatMsg,
	)
	return nil
}

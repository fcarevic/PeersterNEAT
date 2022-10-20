package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// Callback function for chat message
func (n *node) chatMessageCallback(msg types.Message, pkt transport.Packet) error {

	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to Chat message got wrong type: %T", msg)
	}

	// Log received message
	log.Info().Msgf(
		"Source: %s \t Destination: %s: \t MessageType: %s \t MessageBody: %s",
		pkt.Header.Source,
		pkt.Header.Destination,
		pkt.Msg.Type,
		chatMsg)
	return nil
}

// RumorMessageCallback Callback function for Rumor message
func (n *node) RumorMessageCallback(msg types.Message, pkt transport.Packet) error {

	rumorMsg, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to Rumor message got wrong type: %T", msg)
	}

	// Check if the rumor is expected
	expectedRumors, statusMap := n.rumorInfo.filterExpectedRumors(rumorMsg.Rumors)
	if len(expectedRumors) == 0 {
		return nil
	}

	// Update routing table
	// TODO: DO WE CREATE A NEW RUMOR MESSAGE, OR RELAY THE PACKET?
	n.updateRoutingTableWithRumors(expectedRumors, pkt.Header.RelayedBy)

	// Send ACK
	err := n.sendAckForRumorPacket(pkt, statusMap)
	if err != nil {
		log.Error().Msgf("[%s]: RumorMessageCallback: Sending ACK failed", n.conf.Socket.GetAddress())
		return err
	}

	// Send to another neighbours
	errRelay := n.sendRumors(expectedRumors, true, pkt.Header.RelayedBy, "")
	if errRelay != nil {
		return errRelay
	}

	// Process embedded msgs
	for _, rumor := range expectedRumors {
		embeddedMsg := rumor.Msg
		dpkt := transport.Packet{
			Header: pkt.Header,
			Msg:    embeddedMsg,
		}
		errProcess := n.conf.MessageRegistry.ProcessPacket(dpkt)
		if errProcess != nil {
			return errProcess
		}
	}

	// Log received message
	log.Info().Msgf(
		"[%s]: RUMOR CALLBACK, Source: %s \t Destination: %s: \t MessageType: %s \t MessageBody: %s",
		n.conf.Socket.GetAddress(),
		pkt.Header.Source,
		pkt.Header.Destination,
		pkt.Msg.Type,
		rumorMsg)
	return nil
}

// Callback function for ACK message
func (n *node) ackMessageCallback(msg types.Message, pkt transport.Packet) error {

	ackMsg, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to ack message got wrong type: %T", msg)
	}

	// Log received message
	log.Info().Msgf(
		"[%s]: ACK CALLBACK, Source: %s \t Destination: %s: \t MessageType: %s \t MessageBody: %s",
		n.conf.Socket.GetAddress(),
		pkt.Header.Source,
		pkt.Header.Destination,
		pkt.Msg.Type,
		ackMsg)
	return nil
}

// Callback function for Status message
func (n *node) statusMessageCallback(msg types.Message, pkt transport.Packet) error {

	statusMsg, ok := msg.(*types.StatusMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to status message got wrong type: %T", msg)
	}

	// Process the status message
	missing, rumorsToSend := n.processStatus(*statusMsg)

	// If I am missing rumors, send status message to origin
	if missing {
		var myStatusMsg, _ = n.getStatusMaps()
		myAddress := n.conf.Socket.GetAddress()
		myStatusPkt, errConvert := n.msgTypesToPacket(
			myAddress,
			myAddress,
			pkt.Header.Source,
			myStatusMsg)
		if errConvert != nil {
			return errConvert
		}
		err := n.sendPkt(myStatusPkt, TIMEOUT)
		if err != nil {
			log.Error().Msgf("[%s]: statusMessageCallback: Sending status failed", n.conf.Socket.GetAddress())
			return err
		}
	}

	// Send missing rumors to peer
	if len(rumorsToSend) != 0 {
		err := n.sendRumors(rumorsToSend, false, "", pkt.Header.Source)
		if err != nil {
			log.Error().Msgf("[%s]: statusMessageCallback: Sending extra rumors failed", n.conf.Socket.GetAddress())
			return err
		}
	}
	// Log received message
	log.Info().Msgf(
		"[%s]: Status message callback:  Source: %s \t Destination: %s: \t MessageType: %s \t MessageBody: %s",
		n.conf.Socket.GetAddress(),
		pkt.Header.Source,
		pkt.Header.Destination,
		pkt.Msg.Type,
		statusMsg)
	return nil
}

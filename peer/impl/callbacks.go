package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
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
	n.updateRoutingTableWithRumors(expectedRumors, pkt.Header.RelayedBy)

	// Send ACK
	err := n.sendAckForRumorPacket(pkt, statusMap)
	if err != nil {
		log.Error().Msgf("[%s]: RumorMessageCallback: Sending ACK failed", n.conf.Socket.GetAddress())
		return err
	}

	// Send to another neighbours
	errRelay := n.startRumoring(pkt.Copy(), []string{pkt.Header.RelayedBy})
	if errRelay != nil {
		log.Error().Msgf(
			"[%s]: RumorMessageCallback: error: %s ",
			n.conf.Socket.GetAddress(),
			errRelay.Error(),
		)

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

	return nil
}

// Callback function for ACK message
func (n *node) ackMessageCallback(msg types.Message, pkt transport.Packet) error {

	ackMsg, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to ack message got wrong type: %T", msg)
	}

	//log.Info().Msgf(
	//	"[%s]: ackMessageCallback: entered ",
	//	n.conf.Socket.GetAddress(),
	//)

	if !(n.rumorInfo.processAck(pkt, *ackMsg)) {

		// If the ack is not expected by the Rumoring, then it is the part of the continual mongering
		// Extract status msg
		statusMsg := ackMsg.Status

		// Craft new packet
		statusPacket, errS := n.msgTypesToPacket(
			pkt.Header.Source,
			pkt.Header.RelayedBy,
			pkt.Header.Destination,
			statusMsg,
		)
		if errS != nil {
			log.Info().Msgf(
				"[%s]: rumorThread: error processing packet of messageType: %s",
				n.conf.Socket.GetAddress(),
				pkt.Msg.Type,
			)
		}

		// Process pkt
		errProcess := n.conf.MessageRegistry.ProcessPacket(statusPacket)
		if errProcess != nil {
			log.Info().Msgf(
				"[%s]: ackMessageCallback: error processing packet of messageType: %s",
				n.conf.Socket.GetAddress(),
				statusPacket.Msg.Type,
			)
		}
	}

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

	log.Error().Msgf("[%s] %s, len %d , %s", n.conf.Socket.GetAddress(), missing, len(rumorsToSend), rumorsToSend)

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
		err := n.conf.Socket.Send(pkt.Header.Source, myStatusPkt, TIMEOUT)
		log.Error().Msgf(
			"[%s]: statusMessageCallback: Sending request to catch up to %s",
			n.conf.Socket.GetAddress(),
			pkt.Header.Source)
		if err != nil {
			log.Error().Msgf(
				"[%s]: statusMessageCallback: Sending status failed",
				n.conf.Socket.GetAddress(),
			)
			return err
		}
	}

	// Send missing rumors to peer
	if len(rumorsToSend) != 0 {
		err := n.sendCatchUpRumors(rumorsToSend, pkt.Header.Source)
		log.Info().Msgf(
			"[%s]: statusMessageCallback: Sending extra rumors to %s",
			n.conf.Socket.GetAddress(),
			pkt.Header.Source)
		if err != nil {
			log.Error().Msgf(
				"[%s]: statusMessageCallback: Sending extra rumors failed",
				n.conf.Socket.GetAddress())
			return err
		}
	}

	// Continue mongering
	if len(rumorsToSend) == 0 && !missing {
		r := rand.Float64()
		if r <= n.conf.ContinueMongering {

			// Create a status msg
			var statusMsg, _ = n.getStatusMaps()
			// Send to random neigbour
			err := n.sendToRandomNeighbour(statusMsg, []string{pkt.Header.RelayedBy})
			if err != nil {
				log.Info().Msgf("[%s]: statusMessageCallback: continue mongering: %s",
					n.conf.Socket.GetAddress(),
					err.Error(),
				)
			}
		}

	}

	return nil
}

func (n *node) privateMessageCallback(msg types.Message, pkt transport.Packet) error {
	privateMsg, ok := msg.(*types.PrivateMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to Chat message got wrong type: %T", msg)
	}

	// Check if the node is a recipient and process the message
	var _, exist = privateMsg.Recipients[n.conf.Socket.GetAddress()]
	if exist {

		pkt := msgToPacket(pkt.Header.Source, pkt.Header.RelayedBy, pkt.Header.Destination, *privateMsg.Msg)
		err := n.conf.MessageRegistry.ProcessPacket(pkt)
		if err != nil {
			log.Error().Msgf("[%s]: PrivateMessageCallback: Failed in processing private message: %s", privateMsg)
			return err
		}
		log.Info().Msgf("[%s]: PrivateMessageCallback: Processed private message: %s", privateMsg)

	}
	return nil
}

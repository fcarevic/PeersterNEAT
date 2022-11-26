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

// Callback function for Empty message
func (n *node) emptyMessageCallback(msg types.Message, pkt transport.Packet) error {
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
		//log.Error().Msgf(
		//	"[%s]: statusMessageCallback: Sending request to catch up to %s",
		//	n.conf.Socket.GetAddress(),
		//	pkt.Header.Source)
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
		//log.Info().Msgf(
		//	"[%s]: statusMessageCallback: Sending extra rumors to %s",
		//	n.conf.Socket.GetAddress(),
		//	pkt.Header.Source)
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
		if r < n.conf.ContinueMongering {

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
		return xerrors.Errorf("Failed to cast to private message got wrong type: %T", msg)
	}

	// Check if the node is a recipient and process the message
	var _, exist = privateMsg.Recipients[n.conf.Socket.GetAddress()]
	if exist {

		pkt := msgToPacket(pkt.Header.Source, pkt.Header.RelayedBy, pkt.Header.Destination, *privateMsg.Msg)
		err := n.conf.MessageRegistry.ProcessPacket(pkt)
		if err != nil {
			log.Error().Msgf("[%s]: PrivateMessageCallback: Failed in processing private message %s",
				n.conf.Socket.GetAddress(),
				privateMsg)
			return err
		}
		log.Info().Msgf("[%s]: PrivateMessageCallback: Processed private message %s",
			n.conf.Socket.GetAddress(),
			privateMsg)

	}
	return nil
}

func (n *node) dataRequestsMessageCallback(msg types.Message, pkt transport.Packet) error {
	dataRequestMsg, ok := msg.(*types.DataRequestMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to dataaRequestMessage message got wrong type: %T", msg)
	}

	// Get requested data
	data := n.conf.Storage.GetDataBlobStore().Get(dataRequestMsg.Key)

	// Craft msg
	reply := types.DataReplyMessage{
		Key:       dataRequestMsg.Key,
		RequestID: dataRequestMsg.RequestID,
		Value:     data,
	}

	// Send msg
	msgTransport, err := n.conf.MessageRegistry.MarshalMessage(reply)
	if err != nil {
		return err
	}
	errUnicast := n.Unicast(pkt.Header.Source, msgTransport)
	return errUnicast
}

func (n *node) dataReplyMessageCallback(msg types.Message, pkt transport.Packet) error {
	dataReplyMsg, ok := msg.(*types.DataReplyMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to DataReplyMessage message got wrong type: %T", msg)
	}
	log.Info().Msgf("[%s] received data reply from %s",
		n.conf.Socket.GetAddress(), pkt.Header.Source)
	n.processDataReply(*dataReplyMsg, pkt)
	return nil
}

func (n *node) searchRequestMessageCallback(msg types.Message, pkt transport.Packet) error {
	searchRequestMsg, ok := msg.(*types.SearchRequestMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to DataReplyMessage message got wrong type: %T", msg)
	}

	if n.checkDuplicateAndRegister(searchRequestMsg.RequestID) {
		log.Info().Msgf("[%s]: recID %s DUPLIUCATE | relayed from %s | req origin %s",
			n.conf.Socket.GetAddress(), searchRequestMsg.RequestID,
			pkt.Header.RelayedBy, searchRequestMsg.Origin)
		return nil
	}

	n.handleSearchRequestLocally(*searchRequestMsg, pkt)
	log.Info().Msgf("[%s] sent reply for message request to %s",
		n.conf.Socket.GetAddress(), pkt.Header.Source)
	// Update budget
	budget := searchRequestMsg.Budget - 1
	// Forward request
	if budget > 0 {
		n.forwardRequestToNeighbours(budget, *searchRequestMsg, pkt)
	}
	return nil
}

func (n *node) searchReplyMessageCallback(msg types.Message, pkt transport.Packet) error {
	searchReplyMsg, ok := msg.(*types.SearchReplyMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to SearchReplyMessage message got wrong type: %T", msg)
	}

	// Update naming store and catalog
	foundKnown := false
	for _, fileInfo := range searchReplyMsg.Responses {

		// Update naming storage
		errTag := n.Tag(fileInfo.Name, fileInfo.Metahash)
		if errTag != nil {
			log.Error().Msgf("[%s]: searchReplyMessageCallback: Unable to tag: %s",
				n.conf.Socket.GetAddress(),
				errTag.Error())
		}

		// Update catalog
		fullyKnown := true
		n.UpdateCatalog(fileInfo.Metahash, pkt.Header.Source)
		for _, chunkTagByte := range fileInfo.Chunks {
			if chunkTagByte == nil {
				fullyKnown = false
				continue
			}
			chunkTag := string(chunkTagByte)
			n.UpdateCatalog(chunkTag, pkt.Header.Source)
		}
		if !foundKnown && fullyKnown && len(fileInfo.Chunks) > 0 {
			foundKnown = true
			n.notifyFullyKnown(searchReplyMsg.RequestID, fileInfo.Name)
		}
	}
	//log.Info().Msgf("[%s] SearchReplyCallback: %s",
	//	n.conf.Socket.GetAddress(),
	//	searchReplyMsg)

	return nil
}

func (n *node) paxosPrepareMessageCallback(msg types.Message, pkt transport.Packet) error {
	paxosPrepareMsg, ok := msg.(*types.PaxosPrepareMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to PaxosPrepareMessage message got wrong type: %T", msg)
	}

	// Check if message is expected
	if !n.paxosInfo.paxos.isExpectedPaxosPrepareMsg(*paxosPrepareMsg) {
		return nil
	}

	log.Info().Msgf("[%s] received prepare from %s", n.conf.Socket.GetAddress(), pkt.Header.Source)
	n.sendPaxosPromise(*paxosPrepareMsg)
	return nil
}

func (n *node) paxosProposeMessageCallback(msg types.Message, pkt transport.Packet) error {
	paxosProposeMsg, ok := msg.(*types.PaxosProposeMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to PaxosProposeMessage message got wrong type: %T", msg)
	}

	// Check if message is expected
	if !n.paxosInfo.paxos.isExpectedPaxosProposeMsg(*paxosProposeMsg) {
		return nil
	}
	log.Info().Msgf("[%s] received propose from %s", n.conf.Socket.GetAddress(), pkt.Header.Source)
	n.broadcastPaxosAccept(*paxosProposeMsg)
	return nil
}

func (n *node) paxosPromiseMessageCallback(msg types.Message, pkt transport.Packet) error {
	paxosPromiseMsg, ok := msg.(*types.PaxosPromiseMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to PaxosProposeMessage message got wrong type: %T", msg)
	}

	// Check if message is expected
	if !n.paxosInfo.paxos.isExpectedPaxosPromiseMsg(*paxosPromiseMsg) {
		return nil
	}
	log.Info().Msgf("[%s] received promise from %s for ID:%d   step: %d", n.conf.Socket.GetAddress(), pkt.Header.Source, paxosPromiseMsg.ID, paxosPromiseMsg.Step)

	// TODO: what if accepted id is different?
	id := paxosPromiseMsg.ID
	acceptedValue := PaxosToSend{id: id, value: nil}
	if paxosPromiseMsg.AcceptedValue != nil {
		acceptedValue.id = paxosPromiseMsg.AcceptedID
		acceptedValue.value = paxosPromiseMsg.AcceptedValue
	}
	n.paxosInfo.paxos.notifyPreparePaxosID(id, acceptedValue)
	return nil
}

func (n *node) paxosAcceptMessageCallback(msg types.Message, pkt transport.Packet) error {
	paxosAcceptMsg, ok := msg.(*types.PaxosAcceptMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to PaxosProposeMessage message got wrong type: %T", msg)
	}

	// Check if message is expected
	if !n.paxosInfo.paxos.isExpectedPaxosAcceptMsg(*paxosAcceptMsg) {
		return nil
	}

	log.Info().Msgf("[%s] received accept from %s", n.conf.Socket.GetAddress(), pkt.Header.Source)
	// TODO: what if accepted id is different?
	id := paxosAcceptMsg.ID
	n.paxosInfo.paxos.notifyProposePaxosID(id, *paxosAcceptMsg)
	return nil
}

func (n *node) tlcMessageCallback(msg types.Message, pkt transport.Packet) error {
	tlcMsg, ok := msg.(*types.TLCMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to TLCMessage message got wrong type: %T", msg)
	}

	// Check if message is expected
	n.handleTlCMsg(*tlcMsg)

	log.Info().Msgf("[%s] received tlcMsg from %s: step %d , filename: %s ", n.conf.Socket.GetAddress(),
		pkt.Header.Source, tlcMsg.Step, tlcMsg.Block.Value.Filename)
	return nil
}

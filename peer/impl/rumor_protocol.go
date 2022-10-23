package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
	"sync"
	"time"
)

type RumorInfo struct {
	peerSequences   map[string]uint
	peerRumors      map[string][]types.Rumor
	pktToChannelMap map[string]chan struct {
		transport.Packet
		types.AckMessage
	}

	// Semaphores
	rumorInfoMutex    sync.Mutex
	pktToChannelMutex sync.Mutex
}

func (rumorInfo *RumorInfo) registerNewRumor(rumor types.Rumor, origin string) types.Rumor {
	// Acquire lock
	rumorInfo.rumorInfoMutex.Lock()
	defer rumorInfo.rumorInfoMutex.Unlock()

	// Add sequence number to header
	var sequenceCounter, ok = rumorInfo.peerSequences[origin]
	if !ok {
		sequenceCounter = 0
	}
	sequenceCounter = sequenceCounter + 1
	rumor.Sequence = sequenceCounter

	// Insert into map of all rumors
	rumorInfo.peerRumors[origin] = append(rumorInfo.peerRumors[origin], rumor)
	rumorInfo.peerSequences[origin] = sequenceCounter
	return rumor
}

func (rumorInfo *RumorInfo) registerRumorMessageForAck(
	pkt transport.Packet,
	channel chan struct {
	transport.Packet
	types.AckMessage
}) {

	// Acquire lock
	rumorInfo.pktToChannelMutex.Lock()
	defer rumorInfo.pktToChannelMutex.Unlock()

	// Add PacketID to map
	rumorInfo.pktToChannelMap[pkt.Header.PacketID] = channel

}

func (rumorInfo *RumorInfo) unregisterRumorMessageForAck(pkt transport.Packet) {

	// Acquire lock
	rumorInfo.pktToChannelMutex.Lock()
	defer rumorInfo.pktToChannelMutex.Unlock()

	// Remove PacketID from map
	delete(rumorInfo.pktToChannelMap, pkt.Header.PacketID)
}

func (rumorInfo *RumorInfo) processAck(pkt transport.Packet, ack types.AckMessage) bool {

	// Acquire lock
	rumorInfo.pktToChannelMutex.Lock()
	defer rumorInfo.pktToChannelMutex.Unlock()

	var channel, ok = rumorInfo.pktToChannelMap[ack.AckedPacketID]
	if !ok {
		return false
	}

	// Notify arrival of ack
	channel <- struct {
		transport.Packet
		types.AckMessage
	}{pkt, ack}
	return true
}

func (rumorInfo *RumorInfo) getSequenceNumber(peer string) uint {
	// Return sequence number for peer
	var numberOfSequences, ok = rumorInfo.peerSequences[peer]
	if !ok {

		// If it does not exist, initiate
		rumorInfo.peerSequences[peer] = 0
		rumorInfo.peerRumors[peer] = []types.Rumor{}
		return 0
	}
	return numberOfSequences
}

func (rumorInfo *RumorInfo) insertPeerRumor(
	peer string,
	rumor types.Rumor,
) {

	// Increment number of sequence for peer
	var numberOfSequences, ok = rumorInfo.peerSequences[peer]
	if !ok {
		numberOfSequences = 0
	}
	rumorInfo.peerSequences[peer] = numberOfSequences + 1
	rumorInfo.peerRumors[peer] = append(rumorInfo.peerRumors[peer], rumor)
}

// Returns the map of peer-> sequence NUM (including the self node), and the map of peer->[]Rumors
func (n *node) getStatusMaps() (types.StatusMessage, map[string][]types.Rumor) {

	// Acquire lock
	n.rumorInfo.rumorInfoMutex.Lock()
	defer n.rumorInfo.rumorInfoMutex.Unlock()

	// Copy peer map
	var statusMap, rumorMap = copyStatusRumorMaps(n.rumorInfo.peerSequences, n.rumorInfo.peerRumors)

	return statusMap, rumorMap
}

// Process received Status message
// Returns an inidcator if the node is lacking some rumors, and the array of rumors it should send to peer
func (n *node) processStatus(statusMsg types.StatusMessage) (bool, []types.Rumor) {
	myStatusMap, myRumorMap := n.getStatusMaps()
	var (
		missing = false
		toSend  []types.Rumor
	)

	// What is peer missing
	for myPeer, myNum := range myStatusMap {
		var remoteNum, ok = statusMsg[myPeer]
		if !ok {
			toSend = append(toSend, myRumorMap[myPeer]...)
			continue
		}

		if remoteNum == myNum {
			continue
		}
		if remoteNum < myNum {
			for _, rumor := range myRumorMap[myPeer] {
				if rumor.Sequence > remoteNum {
					toSend = append(toSend, rumor)
				}
			}
		} else {
			missing = true
		}
	}

	// What am I missing
	for remotePeer := range statusMsg {
		var _, ok = myStatusMap[remotePeer]
		if !ok {
			missing = true
		}
	}
	return missing, toSend
}

func (rumorInfo *RumorInfo) filterExpectedRumors(rumors []types.Rumor) ([]types.Rumor, map[string]uint) {

	// Acquire lock
	rumorInfo.rumorInfoMutex.Lock()
	defer rumorInfo.rumorInfoMutex.Unlock()

	var validRumors []types.Rumor
	for _, rumor := range rumors {

		lastSequenceNumber := rumorInfo.getSequenceNumber(rumor.Origin)
		if rumor.Sequence == lastSequenceNumber+1 {
			validRumors = append(validRumors, rumor)
			rumorInfo.insertPeerRumor(rumor.Origin, rumor)
		}
	}

	statusMap, _ := copyStatusRumorMaps(rumorInfo.peerSequences, rumorInfo.peerRumors)
	return validRumors, statusMap
}

// Get random neighbour for given node
func (n *node) getRangomNeighbour(notIncluded []string) (string, error) {

	// Acquire lock
	n.routingTableMutex.RLock()
	defer n.routingTableMutex.RUnlock()

	// Extract all neighbours
	// Code taken from https://stackoverflow.com/questions/13422578/in-go-how-to-get-a-slice-of-values-from-a-map
	var peers []string
	for key, value := range n.routingTable {
		// Exclude me
		if key == n.conf.Socket.GetAddress() {
			continue
		}
		// If destination and next hop addresses are the same, it means that the node is a peer
		if key == value {
			if !contains(notIncluded, key) {
				peers = append(peers, value)
			}
		}
	}

	// If there are no peers, return error
	if len(peers) == 0 {
		return "", xerrors.Errorf("[getRandomNeighbour] Empty map of neighbours")
	}

	// Return random neighbour
	return peers[rand.Intn(len(peers))], nil
}

// Sends rumor message for a given node
func (n *node) sendMessageAsRumor(msg transport.Message, excludeNeighbours []string) error {

	// Craft a rumor msg
	var rumorObject = types.Rumor{
		Msg:      &msg,
		Sequence: 0,
		Origin:   n.conf.Socket.GetAddress(),
	}

	// Register rumor to the map and update sequence number.
	rumorObject = n.rumorInfo.registerNewRumor(rumorObject, n.conf.Socket.GetAddress())
	var rumorMessage = types.RumorsMessage{
		Rumors: []types.Rumor{rumorObject}}

	// Get node's address
	srcAddress := n.conf.Socket.GetAddress()

	// Craft a packet
	var packet, errCast = n.msgTypesToPacket(srcAddress, srcAddress, "dummy", rumorMessage)
	if errCast != nil {
		log.Error().Msgf("[%s]: sendRumorMessage: Marshalling failed", n.conf.Socket.GetAddress())
	}
	// Start rumoring service
	return n.startRumoring(packet, excludeNeighbours)
}

func (n *node) sendAckForRumorPacket(pkt transport.Packet, statusMap map[string]uint) error {
	// Get addresses
	receivedFrom := pkt.Header.Source
	myAddress := n.conf.Socket.GetAddress()

	// Craft ACK message
	ackMessage := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        statusMap,
	}

	// Craft packet
	ackPacket, err := n.msgTypesToPacket(myAddress, myAddress, receivedFrom, ackMessage)
	if err != nil {
		log.Error().Msgf("[%s]: sendAckForRumorPacket: Marshalling failed", n.conf.Socket.GetAddress())
		return err
	}

	// Send packet
	errSend := n.conf.Socket.Send(receivedFrom, ackPacket, TIMEOUT)
	if errSend != nil {
		log.Error().Msgf("[%s]: sendAckForRumorPacket: Sending failed: %s", n.conf.Socket.GetAddress(),
			errSend.Error())
		return errSend
	}
	log.Info().Msgf("[%s]: sendAckForRumorPacket: Sent ACK to %s", n.conf.Socket.GetAddress(), receivedFrom)
	return nil
}

func (n *node) updateRoutingTableWithRumors(rumors []types.Rumor, relay string) {
	n.routingTableMutex.Lock()
	defer n.routingTableMutex.Unlock()

	for _, rumor := range rumors {

		src := rumor.Origin
		var nextHop, ok = n.routingTable[src]
		if !ok {
			n.routingTable[src] = relay
			continue
		}
		// If it is the peer, do not update it
		if nextHop == src {
			continue
		}
		n.routingTable[src] = relay
	}
}

// Send rumors as catch-up
func (n *node) sendCatchUpRumors(rumors []types.Rumor, sendTo string) error {

	// Sort the array
	rumors = sortRumors(rumors)

	// Get addresses
	srcAddress := n.conf.Socket.GetAddress()

	// Craft message
	var rumorMessage = types.RumorsMessage{
		Rumors: rumors,
	}
	// Convert to packet
	packet, errCast := n.msgTypesToPacket(srcAddress, srcAddress, sendTo, rumorMessage)
	if errCast != nil {
		log.Error().Msgf("%s: sendRumors: %s", n.conf.Socket.GetAddress(), errCast.Error())
		return errCast
	}

	// Send packet. We do not use routing table for catch-up
	errSend := n.conf.Socket.Send(sendTo, packet, TIMEOUT)
	if errSend != nil {
		log.Error().Msgf("[%s]: sendRumors: Sending w/o routing table failed: %s", n.conf.Socket.GetAddress(),
			errSend.Error())
		return errSend
	}
	return nil
}

func (n *node) changePacketHeaderForRumoring(
	packet transport.Packet,
	exclNeighbors []string) (transport.Packet, error) {
	// Change pkt header
	var newNeighbour, errN = n.getRangomNeighbour(exclNeighbors)
	if errN != nil {
		// If there are no more neighbours, return
		log.Error().Msgf(
			"[%s]:startPacketRumoring: Aborting %s",
			n.conf.Socket.GetAddress(),
			errN.Error())
		return transport.Packet{}, errN
	}

	// Change the packet header to change the packet ID
	var newPacketHeader = transport.NewHeader(
		packet.Header.Source,
		n.conf.Socket.GetAddress(),
		newNeighbour,
		packet.Header.TTL,
	)
	packet.Header = &newPacketHeader

	return packet, nil

}

func (n *node) processMsgByDummyPkt(msg types.Message, src string, relayedBy string, dst string) error {
	statusPacket, errS := n.msgTypesToPacket(
		src,
		relayedBy,
		dst,
		msg,
	)
	if errS != nil {
		return errS
	}
	// Process pkt
	return n.conf.MessageRegistry.ProcessPacket(statusPacket)
}

func (n *node) startRumoring(
	packet transport.Packet,
	exclNeighbors []string,
) error {

	// No timeout, just send the packet
	if n.conf.AckTimeout <= 0 {
		log.Info().Msgf("[%s]: Sending w/0 ack", n.conf.Socket.GetAddress())
		_, _ = n.sendPktToRandomNeighbour(packet, exclNeighbors, false, nil)
		return nil
	}
	// Init
	channel := make(chan struct {
		transport.Packet
		types.AckMessage
	})
	var timer = time.NewTicker(n.conf.AckTimeout)
	n.activeThreads.Add(1)

	go func() {
		// notify the end
		defer func() {
			timer.Stop()
			n.activeThreads.Done()
			close(channel)
			log.Info().Msgf("[%s]: RumorThread: stopped", n.conf.Socket.GetAddress())
		}()
		for n.getRunning() {
			log.Info().Msgf("[%s]: RumorThread: started", n.conf.Socket.GetAddress())
			log.Info().Msgf("[%s]: Sending w ack", n.conf.Socket.GetAddress())
			pkt, err := n.sendPktToRandomNeighbour(packet, exclNeighbors, true, channel)
			if err != nil {
				return
			}
			packet = pkt

			select {
			case pair := <-channel:
				var arrivedPkt, ackMsg = pair.Packet, pair.AckMessage
				if ackMsg.AckedPacketID == packet.Header.PacketID {
					log.Info().Msgf("[%s]: Recv ack from %s", n.conf.Socket.GetAddress(), arrivedPkt.Header.Source)
					// Extract status msg and process it
					statusMsg := ackMsg.Status
					_ = n.processMsgByDummyPkt(
						statusMsg, arrivedPkt.Header.Source, arrivedPkt.Header.RelayedBy, arrivedPkt.Header.Destination)
					return

				}
			case <-timer.C:
				// Unregister this packet
				n.rumorInfo.unregisterRumorMessageForAck(packet)
				// Add this neighbour to list of excluded ones
				exclNeighbors = append(exclNeighbors, packet.Header.Destination)
			}
		}
	}()
	return nil
}

func (n *node) sendPktToRandomNeighbour(pkt transport.Packet, excludePeers []string,
	needAck bool, channel chan struct {
	transport.Packet
	types.AckMessage
}) (transport.Packet, error) {
	// Get random neighbour
	packet, err := n.changePacketHeaderForRumoring(pkt, excludePeers)
	if err != nil {
		return pkt, err
	}

	// Add pktID and channel to a map if needed ack
	if needAck {
		n.rumorInfo.registerRumorMessageForAck(packet, channel)
	}
	// Send the packet
	errSend := n.sendPkt(packet, TIMEOUT)
	if errSend != nil {
		if needAck {
			n.rumorInfo.unregisterRumorMessageForAck(packet)
		}
		log.Error().Msgf("[%s]: sendToRandomNeighbour: Sending message failed", n.conf.Socket.GetAddress())
		return packet, errSend
	}
	return packet, nil
}

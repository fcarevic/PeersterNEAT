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
	return
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
func (n *node) sendMessageAsRumor(msg transport.Message) error {

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

	// Start rumoring service
	return n.startRumoring(rumorMessage)
}

func (n *node) startRumoring(rumorMessage types.RumorsMessage) error {

	// Get node's address
	srcAddress := n.conf.Socket.GetAddress()

	// Get random neighbour
	neighbour, err := n.getRangomNeighbour([]string{})
	if err != nil {
		return err
	}

	// Craft a packet
	var packet, errCast = n.msgTypesToPacket(srcAddress, srcAddress, neighbour, rumorMessage)
	if errCast != nil {
		log.Error().Msgf("[%s]: sendRumorMessage: Marshalling failed", n.conf.Socket.GetAddress())
	}

	// Create channel for ack
	channel := make(chan struct {
		transport.Packet
		types.AckMessage
	})

	// Init list for neighbour exclusion
	var excludeNeighbours []string

	// Init a timer
	var timer *time.Ticker = nil
	if n.conf.AckTimeout > 0 {
		timer = time.NewTicker(n.conf.AckTimeout)
	}

	// Register new thread
	n.activeThreads.Add(1)

	go func() {

		// notify the end
		defer func() {
			if timer != nil {
				timer.Stop()
			}
			log.Info().Msgf("[%s]: RumorThread: stopped",
				n.conf.Socket.GetAddress(),
			)
			n.activeThreads.Done()
			close(channel)
		}()

		// Add pktID and channel to a map only if timeout is set
		if n.conf.AckTimeout > 0 {
			n.rumorInfo.registerRumorMessageForAck(packet, channel)
		}

		// Send the packet
		errSend := n.sendPkt(packet, TIMEOUT)
		if errSend != nil {
			log.Error().Msgf("[%s]: sendRumorMessage: Sending message failed", n.conf.Socket.GetAddress())
		}

		log.Info().Msgf("[%s]: sendRumorMessage: Rumor thread started", n.conf.Socket.GetAddress())

		if n.conf.AckTimeout > 0 {
			for {

				// Check whether to stop the thread
				n.startStopMutex.Lock()
				stop := !n.isRunning
				n.startStopMutex.Unlock()
				if stop {
					return
				}

				select {
				case pair := <-channel:
					var arrivedPkt, ackMsg = pair.Packet, pair.AckMessage
					log.Info().Msgf("[%s]: RumorThread: received from: %s",
						n.conf.Socket.GetAddress(),
						arrivedPkt.Header.Source)
					if ackMsg.AckedPacketID == packet.Header.PacketID {
						// Ack successfully received
						log.Info().Msgf("[%s]: RumorThread: ACK successfully received from: %s",
							n.conf.Socket.GetAddress(),
							arrivedPkt.Header.Source)
						// Extract status msg and process it
						// Extract status msg
						statusMsg := ackMsg.Status

						// Craft new packet
						statusPacket, errS := n.msgTypesToPacket(
							arrivedPkt.Header.Source,
							arrivedPkt.Header.RelayedBy,
							arrivedPkt.Header.Destination,
							statusMsg,
						)
						if errS != nil {
							log.Info().Msgf(
								"[%s]: rumorThread: error processing packet of messageType: %s",
								n.conf.Socket.GetAddress(),
								arrivedPkt.Msg.Type,
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
						return
					}
				case <-timer.C:
					// Unregister this packet
					n.rumorInfo.unregisterRumorMessageForAck(packet)

					// Add this neighbour to list of excluded ones
					excludeNeighbours = append(excludeNeighbours, packet.Header.Destination)

					// Create a new one
					var newNeighbour, errN = n.getRangomNeighbour(excludeNeighbours)

					log.Error().Msgf("[%s]: sendRumorMessage: Marshalling failed", n.conf.Socket.GetAddress())

					if errN != nil {
						// If there are no more neighbours, return
						return
					}

					// Craft a packet
					var newPacket, errCastN = n.msgTypesToPacket(srcAddress, srcAddress, newNeighbour, rumorMessage)
					if errCastN != nil {
						log.Error().Msgf("[%s]: sendRumorMessage: Marshalling failed", n.conf.Socket.GetAddress())
					}
					packet = newPacket
					// Add pktID and channel to a map
					n.rumorInfo.registerRumorMessageForAck(packet, channel)

					// Send the packet
					errSendN := n.sendPkt(packet, TIMEOUT)
					if errSendN != nil {
						log.Error().Msgf("[%s]: sendRumorMessage: Sending message failed", n.conf.Socket.GetAddress())
					}
				}
			}
		}

	}()
	return nil
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

// Send rumors as:
//  1. relay (relay=true, receivedFrom !=”)
//  2. catch-up (relay=true, sendTo!=”)
func (n *node) sendRumors(rumors []types.Rumor, relay bool, receivedFrom string, sendTo string) error {

	// Get addresses
	srcAddress := n.conf.Socket.GetAddress()

	// If we want to send rumors as 'catch-up', then the relay is false
	var dstAddress = sendTo
	if relay {
		neighbour, err := n.getRangomNeighbour([]string{receivedFrom})
		if err != nil {
			log.Info().Msgf("[%s]: sendRumors via relay: %s", n.conf.Socket.GetAddress(), err.Error())
			return nil
		}
		dstAddress = neighbour
	}

	// Craft message
	var rumorMessage = types.RumorsMessage{
		Rumors: rumors,
	}
	// Convert to packet
	packet, errCast := n.msgTypesToPacket(srcAddress, srcAddress, dstAddress, rumorMessage)
	if errCast != nil {
		log.Error().Msgf("%s: sendRumors: %s", n.conf.Socket.GetAddress(), errCast.Error())
		return errCast
	}

	// Send packet
	if relay {
		errStartRumor := n.startRumoring(rumorMessage)
		if errStartRumor != nil {
			log.Error().Msgf("%s: sendRumors: Failed to start rumoring: %s", n.conf.Socket.GetAddress(), errStartRumor.Error())
			return errStartRumor
		}
	} else {
		// Send packet
		// we do not use routing table if not relay
		errSend := n.conf.Socket.Send(dstAddress, packet, TIMEOUT)
		//log.Info().Msgf("[%s] trying to send to %s pkt: %s", n.conf.Socket.GetAddress(), dstAddress, packet)
		if errSend != nil {
			log.Error().Msgf("[%s]: sendRumors: Sending w/o routing table failed: %s", n.conf.Socket.GetAddress(),
				errSend.Error())
			return errSend
		}
	}
	return nil
}

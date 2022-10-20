package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
	"sync"
)

type RumorInfo struct {
	peerSequences map[string]uint
	peerRumors    map[string][]types.Rumor

	// Semaphores
	rumorInfoMutex sync.Mutex
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

	//// Add info for current node if sequence num > 0
	//if n.rumorInfo.sequenceCounter > 0 {
	//	statusMap[n.conf.Socket.GetAddress()] = n.rumorInfo.sequenceCounter
	//}

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
func (n *node) sendMessageAsRumor(msg transport.Message) (transport.Packet, error) {

	// Get node's address
	srcAddress := n.conf.Socket.GetAddress()

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

	neighbour, err := n.getRangomNeighbour([]string{})
	if err != nil {
		return transport.Packet{}, err
	}

	// Craft a packet
	packet, err := n.msgTypesToPacket(srcAddress, srcAddress, neighbour, rumorMessage)
	if err != nil {
		log.Error().Msgf("[%s]: sendRumorMessage: Marshalling failed", n.conf.Socket.GetAddress())
		return transport.Packet{}, err
	}
	// Send the packet
	errSend := n.sendPkt(packet, TIMEOUT)
	if errSend != nil {
		log.Error().Msgf("[%s]: sendRumorMessage: Sending message failed", n.conf.Socket.GetAddress())
		return transport.Packet{}, errSend
	}
	return packet, nil
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
		errSend := n.sendPkt(packet, TIMEOUT)
		if errSend != nil {
			log.Error().Msgf("[%s]: sendRumors via relay: %s", n.conf.Socket.GetAddress(), errSend.Error())
			return errSend
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

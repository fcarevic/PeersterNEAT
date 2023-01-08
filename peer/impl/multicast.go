package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"sync"
)

type MulticastInfo struct {

	// Attributes
	mapMulticastClients map[string][]string

	// Semaphores
	multicastMutex sync.Mutex
}

func (s *MulticastInfo) addMulticastClient(streamID string, client string) error {
	// Acquire lock
	s.multicastMutex.Lock()
	defer s.multicastMutex.Unlock()
	multicastClients, ok := s.mapMulticastClients[streamID]
	if !ok {
		multicastClients = make([]string, 0)
	}
	if contains(multicastClients, client) {
		return xerrors.Errorf("Multicast client already exists.")
	}
	multicastClients = append(multicastClients, client)
	s.mapMulticastClients[streamID] = multicastClients
	log.Info().Msgf("Added multicast client %s for stream %s", client, streamID)
	return nil
}

func (s *MulticastInfo) unregisterMulticastStream(streamID string) {
	// Acquire lock
	s.multicastMutex.Lock()
	defer s.multicastMutex.Unlock()
	delete(s.mapMulticastClients, streamID)
}

func (s *MulticastInfo) GetMulticastClients(streamID string) ([]string, error) {
	// Acquire lock
	s.multicastMutex.Lock()
	defer s.multicastMutex.Unlock()
	multicastClients, ok := s.mapMulticastClients[streamID]
	if !ok {
		return []string{}, xerrors.Errorf("Multicast stream %s does not exist.", streamID)
	}
	return multicastClients, nil
}

func (s *MulticastInfo) removeMulticastClient(streamID string, client string) error {
	// Acquire lock
	s.multicastMutex.Lock()
	defer s.multicastMutex.Unlock()
	multicastClients, ok := s.mapMulticastClients[streamID]
	if !ok {
		return xerrors.Errorf("Multicast stream does not exist.")
	}
	if !contains(multicastClients, client) {
		return xerrors.Errorf("Multicast client does not exist.")
	}
	multicastClients = remove[string](multicastClients, client)
	// TODO: Add send disconnect msg if the list is empty!
	s.mapMulticastClients[streamID] = multicastClients
	return nil
}

func (n *node) Multicast(msg types.MulticastMessage) error {

	// Marshall msg
	transportMsg, errMarshall := n.conf.MessageRegistry.MarshalMessage(msg)
	if errMarshall != nil {
		log.Error().Msgf("[%s] Multicast: Marshalling failed: %s",
			n.conf.Socket.GetAddress(), errMarshall.Error())
		return errMarshall
	}
	// Acquire lock
	n.multicstInfo.multicastMutex.Lock()
	defer n.multicstInfo.multicastMutex.Unlock()

	multicastClients, ok := n.multicstInfo.mapMulticastClients[msg.ID]
	if !ok || len(multicastClients) == 0 {
		//log.Error().Msgf("[%s] Multicast: No clients for stream ID: %s",
		//	n.conf.Socket.GetAddress(), msg.ID)
		return xerrors.Errorf("No clients for stream: %s", msg.ID)
	}

	for _, peer := range multicastClients {
		err := n.Unicast(peer, transportMsg)
		if err != nil {
			log.Error().Msgf("[%s] Multicast: Sending failed for multicast peer: %s, error : %s",
				n.conf.Socket.GetAddress(), peer, err.Error())
		}
	}
	return nil
}

func (n *node) JoinMulticast(streamID string, streamerID string, msg *transport.Message) error {

	// Craft multicast msg
	joinMsg := types.MulticastJoinMessage{
		ID:         streamID,
		Message:    msg,
		StreamerID: streamerID,
		ClientID:   n.conf.Socket.GetAddress(),
	}

	// Marshall
	transportMsg, errMarhsall := n.conf.MessageRegistry.MarshalMessage(joinMsg)
	if errMarhsall != nil {
		return errMarhsall
	}

	// Get next hop
	nextHop := n.getNextHop(streamerID)
	if nextHop == "" {
		return xerrors.Errorf("No next hop for streamer %s", streamerID)
	}

	// Send Unicast to next hop
	err := n.Unicast(nextHop, transportMsg)
	if err != nil {
		return err
	}
	return nil
}

func (n *node) StopMulticast(streamID string, message transport.Message) error {
	// Craft MulticastStopMessage
	multicastStopMsg := types.MulticastStopMessage{
		ID:      streamID,
		Message: &message,
	}
	// Marshall
	transportMsg, errM := n.conf.MessageRegistry.MarshalMessage(multicastStopMsg)
	if errM != nil {
		return errM
	}
	// Broadcast
	err := n.Broadcast(transportMsg)
	if err != nil {
		return err
	}
	return nil
}

// /////// CALLBACKS ////////////////////////////////////////
func (n *node) multicastMessageCallback(msg types.Message, pkt transport.Packet) error {
	multicastMsg, ok := msg.(*types.MulticastMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to MulticastMessage message got wrong type: %T", msg)
	}

	_ = n.Multicast(*multicastMsg)
	//if err != nil {
	//	log.Error().Msgf("[%s] multicastMessageCallback: Error in multicast: %s",
	//		n.conf.Socket.GetAddress(), err.Error())
	//}

	pkt = pkt.Copy()
	pkt.Msg = multicastMsg.Message
	errPr := n.conf.MessageRegistry.ProcessPacket(pkt)
	if errPr != nil {
		log.Error().Msgf("[%s] multicastMessageCallback: Error in processing inner msg: %s",
			n.conf.Socket.GetAddress(), errPr.Error())
	}
	return nil
}

func (n *node) multicastStopMessageCallback(msg types.Message, pkt transport.Packet) error {
	multicastMsg, ok := msg.(*types.MulticastStopMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to MulticastStopMessage message got wrong type: %T", msg)
	}
	// Deallocate resources for multicast
	n.multicstInfo.unregisterMulticastStream(multicastMsg.ID)

	// Handle embedded message
	pkt = pkt.Copy()
	pkt.Msg = multicastMsg.Message
	errPr := n.conf.MessageRegistry.ProcessPacket(pkt)
	if errPr != nil {
		log.Error().Msgf("[%s] multicastStopMessageCallback: Error in processing inner msg: %s",
			n.conf.Socket.GetAddress(), errPr.Error())
	}
	return nil
}

func (n *node) multicastJoinMessageCallback(msg types.Message, pkt transport.Packet) error {
	multicastJoinMsg, ok := msg.(*types.MulticastJoinMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to multicastJoinMessageCallback message got wrong type: %T", msg)
	}

	err := n.multicstInfo.addMulticastClient(multicastJoinMsg.ID, pkt.Header.RelayedBy)
	if err != nil {
		log.Error().Msgf("[%s]multicastJoinMessageCallback: Error while adding a client: %s",
			n.conf.Socket.GetAddress(), err.Error())
		if multicastJoinMsg.StreamerID != n.conf.Socket.GetAddress() {
			return err
		}
	}

	if multicastJoinMsg.StreamerID != n.conf.Socket.GetAddress() {
		nextHop := n.getNextHop(multicastJoinMsg.StreamerID)
		if nextHop != "" {
			transportMsg, errC := n.conf.MessageRegistry.MarshalMessage(multicastJoinMsg)
			if errC != nil {
				return err
			}
			errUnicast := n.Unicast(nextHop, transportMsg)
			if errUnicast != nil {
				return errUnicast
			}
		}
	} else {
		pkt = pkt.Copy()
		pkt.Msg = multicastJoinMsg.Message
		errPr := n.conf.MessageRegistry.ProcessPacket(pkt)
		if errPr != nil {
			log.Error().Msgf("[%s] multicastJoinMessageCallback: Error in processing inner msg: %s",
				n.conf.Socket.GetAddress(), errPr.Error())
		}

	}
	return nil
}

// Init
func (n *node) MulticastInit() {
	n.conf.MessageRegistry.RegisterMessageCallback(types.MulticastMessage{}, n.multicastMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.MulticastJoinMessage{}, n.multicastJoinMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.MulticastStopMessage{}, n.multicastStopMessageCallback)
}

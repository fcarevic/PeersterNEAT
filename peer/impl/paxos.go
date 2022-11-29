package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"sync"
	"time"
)

const (
	PaxosInit           = 0
	PaxosPhase1         = 1
	PaxosPhase2         = 2
	ProposerStopNode    = -1
	ProposerStopClock   = -2
	ProposerError       = -3
	ProposerOk          = -4
	ProposerTimeout     = -5
	ProposerNotOurValue = -6
	ProposerOurValue    = -7
)

type MultiPaxos struct {

	// Counters
	globalClockStep uint

	// Paxos instances
	paxos Paxos
	tlc   TLCInfo

	// Semaphores
	multiPaxosMutex sync.Mutex
}

type PaxosToSend struct {
	id    uint
	value *types.PaxosValue
}

type Paxos struct {

	// Counters
	maxID           uint
	proposerRunning bool

	// Map of channels for IDs
	mapPaxosPrepareIDs   map[uint]chan PaxosToSend
	mapPaxosProposeIDs   map[uint]chan types.PaxosAcceptMessage
	mapPaxosAcceptIDs    map[uint][]types.PaxosAcceptMessage
	notifyEndOfClockStep chan types.BlockchainBlock
	channelSuccCons      chan string

	// Field used only for the proposer
	phase uint

	// Accepted msg
	acceptedID    uint
	acceptedValue *types.PaxosValue
}

func (p *MultiPaxos) getGlobalClockStep() uint {
	// Acquire lock
	p.multiPaxosMutex.Lock()
	defer p.multiPaxosMutex.Unlock()
	return p.globalClockStep
}

//func (p *MultiPaxos) nextGlobalClockStep() {
//	// Acquire lock
//	p.multiPaxosMutex.Lock()
//	defer p.multiPaxosMutex.Unlock()
//	p.nextGlobalClockStepUnsafe()
//}

func (p *MultiPaxos) nextGlobalClockStepUnsafe() {
	p.globalClockStep = p.globalClockStep + 1
	p.paxos.resetPaxosUnsafe()
	p.nextTLCStepUnsafe()
}
func (p *Paxos) resetPaxosUnsafe() {

	// Delete all proposals
	p.phase = PaxosInit
	p.proposerRunning = false
	p.maxID = 0
	p.acceptedID = 0
	p.acceptedValue = nil
	//close(p.notifyEndOfClockStep)
	//p.notifyEndOfClockStep = make(chan types.BlockchainBlock, 5000)

	// Reset maps
	for key := range p.mapPaxosProposeIDs {
		//close(channel)
		delete(p.mapPaxosProposeIDs, key)
	}

	// Delete all prepares
	for key := range p.mapPaxosPrepareIDs {
		delete(p.mapPaxosPrepareIDs, key)
	}
	// Del all accepts
	for key := range p.mapPaxosAcceptIDs {
		delete(p.mapPaxosAcceptIDs, key)
	}
}
func (n *node) processPaxosPrepareMsg(paxosPrepareMsg types.PaxosPrepareMessage) {
	// Acquire lock
	n.multiPaxos.multiPaxosMutex.Lock()
	defer n.multiPaxos.multiPaxosMutex.Unlock()

	log.Info().Msgf("[%s] received prepare  in step %d for step %d",
		n.conf.Socket.GetAddress(), paxosPrepareMsg.Step, n.multiPaxos.globalClockStep)

	check := paxosPrepareMsg.Step == n.multiPaxos.globalClockStep && paxosPrepareMsg.ID > n.multiPaxos.paxos.maxID
	if !check {
		return
	}
	n.multiPaxos.paxos.maxID = paxosPrepareMsg.ID
	n.sendPaxosPromise(paxosPrepareMsg)

}

func (n *node) processPaxosPromiseMsg(paxosPromiseMsg types.PaxosPromiseMessage) {
	// Acquire lock
	n.multiPaxos.multiPaxosMutex.Lock()
	defer n.multiPaxos.multiPaxosMutex.Unlock()
	check := paxosPromiseMsg.Step == n.multiPaxos.globalClockStep
	if !check || (n.multiPaxos.paxos.proposerRunning && n.multiPaxos.paxos.phase != PaxosPhase1) {
		return
	}

	id := paxosPromiseMsg.ID
	acceptedValue := PaxosToSend{id: id, value: nil}
	if paxosPromiseMsg.AcceptedValue != nil {
		acceptedValue.id = paxosPromiseMsg.AcceptedID
		acceptedValue.value = paxosPromiseMsg.AcceptedValue
	}
	channel, ok := n.multiPaxos.paxos.mapPaxosPrepareIDs[id]

	if ok {
		defer func() {
			if err := recover(); err != nil {
				log.Error().Msgf("notifyPaxosID: Panic on sending on the channel")
			}
		}()
		channel <- acceptedValue
		return
	}
}

func (n *node) processPaxosProposeMsg(paxosProposeMsg types.PaxosProposeMessage) {
	// Acquire lock
	n.multiPaxos.multiPaxosMutex.Lock()
	defer n.multiPaxos.multiPaxosMutex.Unlock()

	check := paxosProposeMsg.Step == n.multiPaxos.globalClockStep && paxosProposeMsg.ID == n.multiPaxos.paxos.maxID
	if !check {
		return
	}
	n.broadcastPaxosAcceptUnsafe(paxosProposeMsg.ID, paxosProposeMsg.Step, paxosProposeMsg.Value)
}

func (n *node) processPaxosAcceptMsg(paxosAcceptMsg types.PaxosAcceptMessage, pkt transport.Packet) {
	// Acquire lock
	n.multiPaxos.multiPaxosMutex.Lock()
	defer n.multiPaxos.multiPaxosMutex.Unlock()
	//if !(paxosAcceptMsg.Step == n.multiPaxos.globalClockStep && n.multiPaxos.paxos.phase != PAXOS_PHASE1)
	if !(paxosAcceptMsg.Step == n.multiPaxos.globalClockStep) {
		log.Info().Msgf("[%s] rejected accept msg", n.conf.Socket.GetAddress())

		return
	}
	log.Info().Msgf("[%s] received accept from %s", n.conf.Socket.GetAddress(), pkt.Header.Source)
	id := paxosAcceptMsg.ID
	//channel, ok := n.multiPaxos.paxos.mapPaxosProposeIDs[id]

	// Handle accept msg
	list, exist := n.multiPaxos.paxos.mapPaxosAcceptIDs[id]
	if !exist {
		list = make([]types.PaxosAcceptMessage, 0)
	}
	list = append(list, paxosAcceptMsg)
	n.multiPaxos.paxos.mapPaxosAcceptIDs[id] = list
	if len(list) >= n.conf.PaxosThreshold(n.conf.TotalPeers) {
		block := createBlockchainBlock(paxosAcceptMsg.Step, n.getPreviousHash(), paxosAcceptMsg.Value)
		n.broadcastTLCMessageUnsafe(block)
	}
	//if ok {
	//	channel <- paxosAcceptMsg
	//}
}

func (p *Paxos) getAcceptedValueUnsafe() (uint, *types.PaxosValue) {
	return p.acceptedID, p.acceptedValue
}

func (p *MultiPaxos) setPhaseSafe(phase uint) {
	p.multiPaxosMutex.Lock()
	defer p.multiPaxosMutex.Unlock()
	p.paxos.phase = phase
}
func (p *MultiPaxos) registerPreparePaxosID(id uint, channel chan PaxosToSend) {
	// Acquire lock
	p.multiPaxosMutex.Lock()
	defer p.multiPaxosMutex.Unlock()
	p.paxos.mapPaxosPrepareIDs[id] = channel
}

func (p *MultiPaxos) unregisterPreparePaxosID(id uint) {
	// Acquire lock
	p.multiPaxosMutex.Lock()
	defer p.multiPaxosMutex.Unlock()
	delete(p.paxos.mapPaxosPrepareIDs, id)
}

func (p *MultiPaxos) registerProposePaxosID(id uint, channel chan types.PaxosAcceptMessage) {
	// Acquire lock
	p.multiPaxosMutex.Lock()
	defer p.multiPaxosMutex.Unlock()
	p.paxos.mapPaxosProposeIDs[id] = channel
}
func (p *MultiPaxos) unregisterProposePaxosID(id uint) {
	// Acquire lock
	p.multiPaxosMutex.Lock()
	defer p.multiPaxosMutex.Unlock()
	delete(p.paxos.mapPaxosProposeIDs, id)
}

func (p *Paxos) setAcceptedValueUnsafe(id uint, acceptedValue types.PaxosValue) {
	p.acceptedID = id
	p.acceptedValue = &acceptedValue
}

func (n *node) broadcastPaxosAcceptUnsafe(id uint, step uint, value types.PaxosValue) {

	// Create accept msg
	paxosAcceptMsg := types.PaxosAcceptMessage{
		ID:    id,
		Step:  step,
		Value: value,
	}

	// Set the accepted value for current paxos
	n.multiPaxos.paxos.setAcceptedValueUnsafe(id, value)

	message, errMarshall := n.conf.MessageRegistry.MarshalMessage(paxosAcceptMsg)
	if errMarshall != nil {
		log.Error().Msgf("[%s]: broadcastPaxosAccept: Marshalling failed:  %s",
			n.conf.Socket.GetAddress(), errMarshall.Error())
		return
	}

	// Broadcast
	errBroadcast := n.Broadcast(message)
	if errBroadcast != nil {
		log.Error().Msgf("[%s]: broadcastPaxosAccept: Broadcasting falied:  %s",
			n.conf.Socket.GetAddress(), errBroadcast.Error())
		return
	}
}

func (n *node) sendPaxosPromise(paxosPrepareMsg types.PaxosPrepareMessage) {

	// Create paxos promise msg
	paxosPromiseMsg := types.PaxosPromiseMessage{
		ID:   paxosPrepareMsg.ID,
		Step: paxosPrepareMsg.Step,
	}
	accID, accVal := n.multiPaxos.paxos.getAcceptedValueUnsafe()
	if accVal != nil {
		paxosPromiseMsg.AcceptedID = accID
		paxosPromiseMsg.AcceptedValue = accVal
	}

	// Marshall Paxos msg
	messagePaxos, errMarshall := n.conf.MessageRegistry.MarshalMessage(paxosPromiseMsg)
	if errMarshall != nil {
		log.Error().Msgf("[%s]: sendPaxosPromise: Marshalling failed:  %s",
			n.conf.Socket.GetAddress(), errMarshall.Error())
		return
	}

	recepient := make(map[string]struct{})
	recepient[paxosPrepareMsg.Source] = struct{}{}

	// Encapsulate in the private msg
	privateMsg := types.PrivateMessage{
		Recipients: recepient,
		Msg:        &messagePaxos,
	}

	// Marshall private msg
	messagePrivate, errMarshallPrivate := n.conf.MessageRegistry.MarshalMessage(privateMsg)
	if errMarshallPrivate != nil {
		log.Error().Msgf("[%s]: sendPaxosPromise: Marshalling failed:  %s",
			n.conf.Socket.GetAddress(), errMarshallPrivate.Error())
		return
	}

	// Broadcast
	errBroadcast := n.Broadcast(messagePrivate)
	if errBroadcast != nil {
		log.Error().Msgf("[%s]: sendPaxosPromise: Broadcasting falied:  %s",
			n.conf.Socket.GetAddress(), errBroadcast.Error())
		return
	}
}

func (n *node) sendPrepareMessage(step uint, id *uint) (int, PaxosToSend, error) {

	var i uint
	retVal := PaxosToSend{}
	channel := make(chan PaxosToSend, 5000)

	defer func(channel chan PaxosToSend, n *node) {
		close(channel)
		n.multiPaxos.unregisterPreparePaxosID(*id)
	}(channel, n)

	for {
		// Init counters for iteration
		var cntAccepts int

		// Get ID  and register channel
		*id = n.conf.PaxosID + i*n.conf.TotalPeers
		retVal.id = *id
		n.multiPaxos.registerPreparePaxosID(*id, channel)
		log.Info().Msgf("[%s] broadcasting prepare", n.conf.Socket.GetAddress())

		eerBroadcast := n.broadcastPrepareMsg(step, id)
		if eerBroadcast != nil {
			return ProposerError, PaxosToSend{}, eerBroadcast
		}

		log.Info().Msgf("[%s] broadcasted prepare", n.conf.Socket.GetAddress())

		flag := false
		for !flag {
			select {
			case acc := <-channel:
				retVal = updateRetval(acc, retVal)
				cntAccepts++
				if cntAccepts >= n.conf.PaxosThreshold(n.conf.TotalPeers) {
					//n.multiPaxos.setPhaseSafe(PAXOS_PHASE2)
					return ProposerOk, retVal, nil
				}
				break
			case <-n.notifyEnd:
				return ProposerStopNode, PaxosToSend{}, xerrors.Errorf("Stopping node")

			case block := <-n.multiPaxos.paxos.notifyEndOfClockStep:
				log.Info().Msgf("[%s] End of clock in send prepare", n.conf.Socket.GetAddress())
				return ProposerStopClock, PaxosToSend{value: &block.Value}, nil

			case <-time.After(n.conf.PaxosProposerRetry):
				n.multiPaxos.unregisterPreparePaxosID(*id)
				log.Info().Msgf("[%s] Timeout", n.conf.Socket.GetAddress())
				flag = true
				break
			}
		}
		i++
	}
}

func updateRetval(acc PaxosToSend, retVal PaxosToSend) PaxosToSend {
	if (acc.value != nil && retVal.value == nil) || (acc.id > retVal.id) {
		retVal.id = acc.id
		retVal.value = acc.value
	}
	return retVal
}

func (n *node) broadcastPrepareMsg(step uint, id *uint) error {
	// craft Message
	paxosPrepareMessage := types.PaxosPrepareMessage{
		Step:   step,
		ID:     *id,
		Source: n.conf.Socket.GetAddress(),
	}

	// Marshall message
	transportMsg, err := n.conf.MessageRegistry.MarshalMessage(paxosPrepareMessage)
	if err != nil {
		log.Error().Msgf("[%s]: sendPrepareMessage: Error marshalling: %s",
			n.conf.Socket.GetAddress(), err.Error())
		return err
	}

	// Broadcast
	errBroadcast := n.Broadcast(transportMsg)
	if errBroadcast != nil {
		log.Error().Msgf("[%s]: sendPrepareMessage: Broadcast failed: %s",
			n.conf.Socket.GetAddress(), errBroadcast.Error())
		return errBroadcast
	}
	return nil
}
func (n *node) sendProposeMsg(toSend PaxosToSend, step uint) (int, *types.PaxosValue, error) {

	// Craft Message
	proposeMsg := types.PaxosProposeMessage{
		Step:  step,
		ID:    toSend.id,
		Value: *toSend.value,
	}

	// Marshall
	transportMsg, errMarshall := n.conf.MessageRegistry.MarshalMessage(proposeMsg)
	if errMarshall != nil {
		log.Error().Msgf("[%s]: sendProposeMsg: Error marshalling: %s",
			n.conf.Socket.GetAddress(), errMarshall.Error())
		return ProposerError, nil, errMarshall
	}

	endStepChannel := n.multiPaxos.paxos.notifyEndOfClockStep

	// Broadcast
	errBroadcast := n.Broadcast(transportMsg)
	if errBroadcast != nil {
		log.Error().Msgf("[%s]: sendProposeMsg: Broadcast failed: %s",
			n.conf.Socket.GetAddress(), errBroadcast.Error())
		return ProposerError, nil, errBroadcast
	}

	channel := make(chan types.PaxosAcceptMessage, 5000)
	n.multiPaxos.registerProposePaxosID(toSend.id, channel)
	//receivedAccepts := make(map[string][]types.PaxosValue)

	defer func(toSend PaxosToSend, n *node, channel chan types.PaxosAcceptMessage) {
		n.multiPaxos.unregisterProposePaxosID(toSend.id)
		close(channel)
	}(toSend, n, channel)

	for {
		select {
		case <-channel:
			break
		//case acceptMsg := <-channel:
		//	cnt := updateAcceptConsensusMap(&receivedAccepts, acceptMsg)
		//	if cnt >= n.conf.PaxosThreshold(n.conf.TotalPeers) {
		//		return ProposerOk, &acceptMsg.Value, nil
		//	}
		//	break
		case <-n.notifyEnd:
			return ProposerStopNode, nil, xerrors.Errorf("Stopping node")

		case block := <-endStepChannel:
			return ProposerStopClock, &block.Value, nil //xerrors.Errorf("Stopping clock step")
		case <-time.After(n.conf.PaxosProposerRetry):
			return ProposerTimeout, nil, nil

		}
	}
}

//func updateAcceptConsensusMap(acceptMap *map[string][]types.PaxosValue, accptedMsg types.PaxosAcceptMessage) int {
//	list, ok := (*acceptMap)[accptedMsg.Value.UniqID]
//	if !ok {
//		list = make([]types.PaxosValue, 0)
//	}
//	list = append(list, accptedMsg.Value)
//	(*acceptMap)[accptedMsg.Value.UniqID] = list
//	return len(list)
//}

// Returns error and indicator if our value has been accepted
func (n *node) runConsensus(value types.PaxosValue) (int, error) {

	var id uint
	// Send prepare
	for {
		step := n.multiPaxos.getGlobalClockStep()
		log.Info().Msgf("[%s]: runConsensus: before first paxos phase", n.conf.Socket.GetAddress())

		n.multiPaxos.setPhaseSafe(PaxosPhase1)
		status, toSend, errorPrepare := n.sendPrepareMessage(step, &id)
		switch status {
		case ProposerError:
			n.multiPaxos.setPhaseSafe(PaxosInit)
			return status, errorPrepare
		case ProposerStopNode:
			return status, nil
		case ProposerStopClock:
			if toSend.value != nil && (value.Metahash == toSend.value.Metahash && value.Filename == toSend.value.Filename) {
				return ProposerOurValue, nil
			}
			return ProposerStopClock, nil
		}

		n.multiPaxos.setPhaseSafe(PaxosPhase2)
		if toSend.value == nil {
			toSend.value = &value
		}
		log.Info().Msgf("[%s]: runConsensus: before second paxos phase", n.conf.Socket.GetAddress())
		status2, acceptedValue, errPropose := n.sendProposeMsg(toSend, step)

		switch status2 {
		case ProposerError:
			log.Info().Msgf("[%s]: runConsensus: finished with error", n.conf.Socket.GetAddress())
			return status2, errPropose
		case ProposerStopNode:
			return status2, nil
		//case ProposerStopClock:
		//	return status2, nil
		case ProposerTimeout:
			n.multiPaxos.setPhaseSafe(PaxosPhase1)
			continue
		}
		n.multiPaxos.setPhaseSafe(PaxosInit)
		log.Info().Msgf("[%s]: runConsensus: paxos finished", n.conf.Socket.GetAddress())
		isOurValue := value.Metahash == acceptedValue.Metahash && value.Filename == acceptedValue.Filename
		if isOurValue {
			return ProposerOurValue, nil
		}
		return ProposerNotOurValue, nil
	}
}

func (p *MultiPaxos) notifySuccessfulConsensus() {
	p.multiPaxosMutex.Lock()
	defer p.multiPaxosMutex.Unlock()
	p.paxos.proposerRunning = false
	p.paxos.phase = PaxosInit
	close(p.paxos.channelSuccCons)
	p.paxos.channelSuccCons = make(chan string)
}

func (p *MultiPaxos) isProposerRunning() (bool, *chan string) {
	p.multiPaxosMutex.Lock()
	defer p.multiPaxosMutex.Unlock()
	if p.paxos.proposerRunning {
		return true, &p.paxos.channelSuccCons
	}
	p.paxos.proposerRunning = true
	return false, nil
	//return p.proposerRunning
}

func (n *node) getPreviousHash() string {
	if n.conf.Storage.GetBlockchainStore().Len() == 0 {
		bytes := make([]byte, 32)
		for i := 0; i < 32; i++ {
			bytes[i] = 0
		}
		return string(bytes)

	}
	return string(n.conf.Storage.GetBlockchainStore().Get(storage.LastBlockKey))

}

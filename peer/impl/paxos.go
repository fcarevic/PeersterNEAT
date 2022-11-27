package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"sync"
	"time"
)

const (
	PAXOS_INIT             = 0
	PAXOS_PHASE1           = 1
	PAXOS_PHASE2           = 2
	PROPOSER_STOP_NODE     = -1
	PROPOSER_STOP_CLOCK    = -2
	PROPOSER_ERROR         = -3
	PROPOSER_OK            = -4
	PROPOSER_TIMEOUT       = -5
	PROPOSER_NOT_OUR_VALUE = -6
	PROPOSER_OUR_VALUE     = -7
)

type PaxosInfo struct {

	// Counters
	globalClockStep uint

	// Paxos instances
	paxos Paxos

	// Semaphores
	paxosInfoMutex sync.Mutex
}

type PaxosToSend struct {
	id    uint
	value *types.PaxosValue
}

type Paxos struct {

	// Counters
	clockStep       uint
	maxID           uint
	proposerRunning bool

	// Map of channels for IDs
	mapPaxosPrepareIDs   map[uint]chan PaxosToSend
	mapPaxosProposeIDs   map[uint]chan types.PaxosAcceptMessage
	mapPaxosAcceptIDs    map[uint][]types.PaxosAcceptMessage
	notifyEndOfClockStep chan string
	channelSuccCons      chan string

	// Field used only for the proposer
	phase uint

	// Accepted msg
	acceptedID    uint
	acceptedValue *types.PaxosValue

	// Semaphores
	paxosMutex              sync.Mutex
	acceptSemaphore         sync.Mutex
	mapPaxosProposeIDsMutex sync.Mutex
	mapPaxosPrepareIDsMutex sync.Mutex
}

func (p *PaxosInfo) getGlobalClockStep() uint {
	// Acquire lock
	p.paxosInfoMutex.Lock()
	defer p.paxosInfoMutex.Unlock()
	return p.globalClockStep
}

func (p *PaxosInfo) incrementGlobalClockStep() {
	// Acquire lock
	p.paxosInfoMutex.Lock()
	defer p.paxosInfoMutex.Unlock()
	p.globalClockStep = p.globalClockStep + 1
	p.paxos.resetPaxos(p.globalClockStep)

}

func (p *Paxos) resetPaxos(clockStep uint) {

	// Acquire all locks
	p.paxosMutex.Lock()
	defer p.paxosMutex.Unlock()
	p.mapPaxosPrepareIDsMutex.Lock()
	defer p.mapPaxosPrepareIDsMutex.Unlock()
	p.mapPaxosProposeIDsMutex.Lock()
	defer p.mapPaxosProposeIDsMutex.Unlock()
	p.acceptSemaphore.Lock()
	defer p.acceptSemaphore.Unlock()

	// Delete all proposals
	p.phase = PAXOS_INIT
	p.proposerRunning = false
	p.maxID = 0
	p.clockStep = clockStep
	p.acceptedID = 0
	p.acceptedValue = nil
	close(p.notifyEndOfClockStep)
	p.notifyEndOfClockStep = make(chan string, 5000)

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
	for key, _ := range p.mapPaxosAcceptIDs {
		delete(p.mapPaxosAcceptIDs, key)
	}
}
func (p *Paxos) isExpectedPaxosPrepareMsg(paxosPrepareMsg types.PaxosPrepareMessage) bool {
	// Acquire lock
	p.paxosMutex.Lock()
	defer p.paxosMutex.Unlock()
	check := paxosPrepareMsg.Step == p.clockStep && paxosPrepareMsg.ID > p.maxID
	if check {
		p.maxID = paxosPrepareMsg.ID
	}
	return check
}

func (p *Paxos) isExpectedPaxosPromiseMsg(paxosPromiseMsg types.PaxosPromiseMessage) bool {
	// Acquire lock
	p.paxosMutex.Lock()
	defer p.paxosMutex.Unlock()
	check := paxosPromiseMsg.Step == p.clockStep && p.phase == PAXOS_PHASE1
	return check
}

func (p *Paxos) isExpectedPaxosProposeMsg(paxosProposeMsg types.PaxosProposeMessage) bool {
	// Acquire lock
	p.paxosMutex.Lock()
	defer p.paxosMutex.Unlock()
	return paxosProposeMsg.Step == p.clockStep && paxosProposeMsg.ID == p.maxID
}

func (p *Paxos) isExpectedPaxosAcceptMsg(paxosAcceptMsg types.PaxosAcceptMessage) bool {
	// Acquire lock
	p.paxosMutex.Lock()
	defer p.paxosMutex.Unlock()
	return paxosAcceptMsg.Step == p.clockStep && p.phase != PAXOS_PHASE1
}

func (p *Paxos) getAcceptedValue() (uint, *types.PaxosValue) {
	// Acquire lock
	p.acceptSemaphore.Lock()
	defer p.acceptSemaphore.Unlock()
	return p.acceptedID, p.acceptedValue
}

func (p *Paxos) getPhase() uint {
	// Acquire lock
	p.paxosMutex.Lock()
	defer p.paxosMutex.Unlock()
	return p.phase
}
func (p *Paxos) setPhase(phase uint) {
	// Acquire lock
	p.paxosMutex.Lock()
	defer p.paxosMutex.Unlock()
	p.phase = phase
}
func (p *Paxos) registerPreparePaxosID(id uint, channel chan PaxosToSend) {
	// Acquire lock
	p.mapPaxosPrepareIDsMutex.Lock()
	defer p.mapPaxosPrepareIDsMutex.Unlock()
	p.mapPaxosPrepareIDs[id] = channel
}

func (p *Paxos) unregisterPreparePaxosID(id uint) {
	// Acquire lock
	p.mapPaxosPrepareIDsMutex.Lock()
	defer p.mapPaxosPrepareIDsMutex.Unlock()
	delete(p.mapPaxosPrepareIDs, id)
}

func (p *Paxos) notifyPreparePaxosID(id uint, toSend PaxosToSend) {
	defer func() {
		if err := recover(); err != nil {
			log.Error().Msgf("notifyPaxosID: Panic on sending on the channel")
		}
	}()
	// Acquire lock
	p.mapPaxosPrepareIDsMutex.Lock()
	channel, ok := p.mapPaxosPrepareIDs[id]
	p.mapPaxosPrepareIDsMutex.Unlock()
	if ok {
		channel <- toSend
	}
}

func (p *Paxos) registerProposePaxosID(id uint, channel chan types.PaxosAcceptMessage) {
	// Acquire lock
	p.mapPaxosProposeIDsMutex.Lock()
	defer p.mapPaxosProposeIDsMutex.Unlock()
	p.mapPaxosProposeIDs[id] = channel
}
func (p *Paxos) unregisterProposePaxosID(id uint) {
	// Acquire lock
	p.mapPaxosProposeIDsMutex.Lock()
	defer p.mapPaxosProposeIDsMutex.Unlock()
	delete(p.mapPaxosProposeIDs, id)
}

func (n *node) notifyProposePaxosID(id uint, acceptMsg types.PaxosAcceptMessage) {
	defer func() {
		if err := recover(); err != nil {
			log.Error().Msgf("notifyPaxosID: Panic on sending on the channel")
		}
	}()
	// Acquire lock
	p := n.paxosInfo.paxos
	p.mapPaxosProposeIDsMutex.Lock()
	channel, ok := p.mapPaxosProposeIDs[id]

	// Handle accept msg
	list, exist := p.mapPaxosAcceptIDs[id]
	if !exist {
		list = make([]types.PaxosAcceptMessage, 0)
	}
	list = append(list, acceptMsg)
	p.mapPaxosAcceptIDs[id] = list
	if len(list) >= n.conf.PaxosThreshold(n.conf.TotalPeers) {
		n.broadcastTLCMessage(createBlockchainBlock(acceptMsg.Step, n.getPreviousHash(), acceptMsg.Value))
	}
	p.mapPaxosProposeIDsMutex.Unlock()

	if ok {
		channel <- acceptMsg
	}
}

func (p *Paxos) setAcceptedValue(id uint, acceptedValue types.PaxosValue) {
	// Acquire lock
	p.acceptSemaphore.Lock()
	defer p.acceptSemaphore.Unlock()
	p.acceptedID = id
	p.acceptedValue = &acceptedValue
}

func (n *node) broadcastPaxosAccept(paxosProposeMsg types.PaxosProposeMessage) {

	// Create accept msg
	paxosAcceptMsg := types.PaxosAcceptMessage{
		ID:    paxosProposeMsg.ID,
		Step:  paxosProposeMsg.Step,
		Value: paxosProposeMsg.Value,
	}

	// Set the accepted value for current paxos
	n.paxosInfo.paxos.setAcceptedValue(paxosProposeMsg.ID, paxosProposeMsg.Value)

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
	accID, accVal := n.paxosInfo.paxos.getAcceptedValue()
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

func (n *node) sendPrepareMessage(step uint, id *uint) (error, int, PaxosToSend) {

	//  TODO: Change for multi-paxos
	var i uint
	retVal := PaxosToSend{}
	channel := make(chan PaxosToSend)

	defer func(channel chan PaxosToSend, n *node) {
		close(channel)
		n.paxosInfo.paxos.unregisterPreparePaxosID(*id)
	}(channel, n)

	for {
		// Init counters for iteration
		var cntAccepts int

		// Get ID  and register channel
		*id = n.conf.PaxosID + i*n.conf.TotalPeers
		retVal.id = *id
		n.paxosInfo.paxos.registerPreparePaxosID(*id, channel)

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
			return err, PROPOSER_ERROR, PaxosToSend{}
		}

		// Broadcast
		errBroadcast := n.Broadcast(transportMsg)
		if errBroadcast != nil {
			log.Error().Msgf("[%s]: sendPrepareMessage: Broadcast failed: %s",
				n.conf.Socket.GetAddress(), errBroadcast.Error())
			// TODO: what happens here??
			return errBroadcast, PROPOSER_ERROR, PaxosToSend{}
		}
		flag := false
		for !flag {
			select {
			case acc := <-channel:
				if (acc.value != nil && retVal.value == nil) || (acc.id > retVal.id) {
					retVal.id = acc.id
					retVal.value = acc.value
				}
				cntAccepts++
				log.Info().Msgf("[%s] ack received promise in sendPrepare", n.conf.Socket.GetAddress())
				if cntAccepts >= n.conf.PaxosThreshold(n.conf.TotalPeers) {
					n.paxosInfo.paxos.setPhase(PAXOS_PHASE2)
					return nil, PROPOSER_OK, retVal
				}
				break
			case <-n.notifyEnd:
				return xerrors.Errorf("Stopping node"), PROPOSER_STOP_NODE, PaxosToSend{}

			case <-n.paxosInfo.paxos.notifyEndOfClockStep:
				return xerrors.Errorf("Stopping clock step"), PROPOSER_STOP_CLOCK, PaxosToSend{}

			case <-time.After(n.conf.PaxosProposerRetry):
				n.paxosInfo.paxos.unregisterPreparePaxosID(*id)
				log.Info().Msgf("[%s] Timeout", n.conf.Socket.GetAddress())
				flag = true
				break
			}
		}
		i++
	}
}

func (n *node) sendProposeMsg(toSend PaxosToSend, step uint) (error, int, *types.PaxosValue) {

	// Craft Message
	// TODO: WHICH ID DO YOU PUT HERE?? ACCEPTED ID OR YOUR ORIGINAL ID?
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
		return errMarshall, PROPOSER_ERROR, nil
	}

	// Broadcast
	errBroadcast := n.Broadcast(transportMsg)
	if errBroadcast != nil {
		log.Error().Msgf("[%s]: sendProposeMsg: Broadcast failed: %s",
			n.conf.Socket.GetAddress(), errBroadcast.Error())
		// TODO: what happens here??
		return errBroadcast, PROPOSER_ERROR, nil
	}

	channel := make(chan types.PaxosAcceptMessage)
	n.paxosInfo.paxos.registerProposePaxosID(toSend.id, channel)
	receivedAccepts := make(map[string][]types.PaxosValue)

	defer func(toSend PaxosToSend, n *node, channel chan types.PaxosAcceptMessage) {
		n.paxosInfo.paxos.unregisterProposePaxosID(toSend.id)
		close(channel)
	}(toSend, n, channel)

	for {
		select {
		case acceptMsg := <-channel:
			cnt := updateAcceptConsensusMap(&receivedAccepts, acceptMsg)
			if cnt >= n.conf.PaxosThreshold(n.conf.TotalPeers) {
				return nil, PROPOSER_OK, &acceptMsg.Value
			}
			break
		case <-n.notifyEnd:
			return xerrors.Errorf("Stopping node"), PROPOSER_STOP_NODE, nil

		case <-n.paxosInfo.paxos.notifyEndOfClockStep:
			return xerrors.Errorf("Stopping clock step"), PROPOSER_STOP_CLOCK, nil
		case <-time.After(n.conf.PaxosProposerRetry):
			return nil, PROPOSER_TIMEOUT, nil

		}
	}
}
func updateAcceptConsensusMap(acceptMap *map[string][]types.PaxosValue, accptedMsg types.PaxosAcceptMessage) int {
	list, ok := (*acceptMap)[accptedMsg.Value.UniqID]
	if !ok {
		list = make([]types.PaxosValue, 0)
	}
	list = append(list, accptedMsg.Value)
	(*acceptMap)[accptedMsg.Value.UniqID] = list
	return len(list)
}

// Returns error and indicator if our value has been accepted
func (n *node) runConsensus(value types.PaxosValue) (error, int) {

	var id uint
	step := n.paxosInfo.getGlobalClockStep()
	// Send prepare
	for {
		log.Info().Msgf("[%s]: runConsensus: before first paxos phase", n.conf.Socket.GetAddress())
		n.paxosInfo.paxos.setPhase(PAXOS_PHASE1)
		errorPrepare, status, toSend := n.sendPrepareMessage(step, &id)
		if errorPrepare != nil {
			n.paxosInfo.paxos.setPhase(PAXOS_INIT)
			return errorPrepare, status
		}
		if status == PROPOSER_STOP_NODE || status == PROPOSER_STOP_CLOCK {
			return nil, status
		}
		n.paxosInfo.paxos.setPhase(PAXOS_PHASE2)
		if toSend.value == nil {
			toSend.value = &value
		}
		log.Info().Msgf("[%s]: runConsensus: before second paxos phase", n.conf.Socket.GetAddress())
		errPropose, status2, acceptedValue := n.sendProposeMsg(toSend, step)

		if errPropose != nil {
			log.Info().Msgf("[%s]: runConsensus: finished with error", n.conf.Socket.GetAddress())
			return errPropose, status
		}

		if status2 == PROPOSER_STOP_NODE || status2 == PROPOSER_STOP_CLOCK {
			return nil, status
		}

		if status2 == PROPOSER_TIMEOUT {
			n.paxosInfo.paxos.setPhase(PAXOS_PHASE1)
			continue
		}
		n.paxosInfo.paxos.setPhase(PAXOS_INIT)
		log.Info().Msgf("[%s]: runConsensus: paxos finished", n.conf.Socket.GetAddress())
		//n.broadcastTLCMessage(createBlockchainBlock(n.paxosInfo.getGlobalClockStep(),
		//	n.getPreviousHash(), *acceptedValue))
		isOurValue := value.Metahash == acceptedValue.Metahash && value.Filename == acceptedValue.Filename
		if isOurValue {
			return nil, PROPOSER_OUR_VALUE
		}
		return nil, PROPOSER_NOT_OUR_VALUE
	}
}

func (p *Paxos) notifySuccessfulConsensus() {
	p.paxosMutex.Lock()
	defer p.paxosMutex.Unlock()
	p.proposerRunning = false
	p.phase = PAXOS_INIT
	close(p.channelSuccCons)
	p.channelSuccCons = make(chan string)
}

func (p *Paxos) isProposerRunning() bool {
	p.paxosMutex.Lock()
	defer p.paxosMutex.Unlock()
	//if p.proposerRunning {
	//	return true, p.channelSuccCons
	//}
	//p.proposerRunning = true
	//return false, p.channelSuccCons
	return p.proposerRunning
}

func (n *node) getPreviousHash() string {
	if n.conf.Storage.GetBlockchainStore().Len() == 0 {
		bytes := make([]byte, 32)
		for i := 0; i < 32; i++ {
			bytes[i] = 0
		}
		return string(bytes)

	} else {
		return string(n.conf.Storage.GetBlockchainStore().Get(storage.LastBlockKey))
	}
}

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
	notifyEndOfClockStep chan string
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

func (p *MultiPaxos) nextGlobalClockStep() {
	// Acquire lock
	p.multiPaxosMutex.Lock()
	defer p.multiPaxosMutex.Unlock()
	p.nextGlobalClockStepUnsafe()
}

func (p *MultiPaxos) nextGlobalClockStepUnsafe() {
	p.globalClockStep = p.globalClockStep + 1
	p.paxos.resetPaxosUnsafe(p.globalClockStep)
	p.nextTLCStepUnsafe()
}
func (p *Paxos) resetPaxosUnsafe(clockStep uint) {

	// Delete all proposals
	p.phase = PAXOS_INIT
	p.proposerRunning = false
	p.maxID = 0
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
func (n *node) processPaxosPrepareMsg(paxosPrepareMsg types.PaxosPrepareMessage) {
	// Acquire lock
	n.multiPaxos.multiPaxosMutex.Lock()
	check := paxosPrepareMsg.Step == n.multiPaxos.globalClockStep && paxosPrepareMsg.ID > n.multiPaxos.paxos.maxID
	if !check {
		n.multiPaxos.multiPaxosMutex.Unlock()
		return
	}
	n.multiPaxos.paxos.maxID = paxosPrepareMsg.ID
	n.sendPaxosPromise(paxosPrepareMsg)
	n.multiPaxos.multiPaxosMutex.Unlock()
}

func (n *node) processPaxosPromiseMsg(paxosPromiseMsg types.PaxosPromiseMessage) {
	// Acquire lock
	n.multiPaxos.multiPaxosMutex.Lock()
	check := paxosPromiseMsg.Step == n.multiPaxos.globalClockStep && n.multiPaxos.paxos.phase == PAXOS_PHASE1
	if !check {
		n.multiPaxos.multiPaxosMutex.Unlock()
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
		n.multiPaxos.multiPaxosMutex.Unlock()
		channel <- acceptedValue
		return
	}
	n.multiPaxos.multiPaxosMutex.Unlock()
}

func (n *node) processPaxosProposeMsg(paxosProposeMsg types.PaxosProposeMessage) {
	// Acquire lock
	n.multiPaxos.multiPaxosMutex.Lock()

	check := paxosProposeMsg.Step == n.multiPaxos.globalClockStep && paxosProposeMsg.ID == n.multiPaxos.paxos.maxID
	if !check {
		n.multiPaxos.multiPaxosMutex.Unlock()
		return
	}
	n.broadcastPaxosAcceptUnsafe(paxosProposeMsg)
	n.multiPaxos.multiPaxosMutex.Unlock()
}

func (n *node) processPaxosAcceptMsg(paxosAcceptMsg types.PaxosAcceptMessage, pkt transport.Packet) {
	// Acquire lock
	n.multiPaxos.multiPaxosMutex.Lock()
	//if !(paxosAcceptMsg.Step == n.multiPaxos.globalClockStep && n.multiPaxos.paxos.phase != PAXOS_PHASE1)
	if !(paxosAcceptMsg.Step == n.multiPaxos.globalClockStep) {
		log.Info().Msgf("[%s] Rejected accept from %s in phase %d and step %d", n.conf.Socket.GetAddress(), pkt.Header.Source, n.multiPaxos.paxos.phase, n.multiPaxos.globalClockStep)
		n.multiPaxos.multiPaxosMutex.Unlock()
		return
	}
	log.Info().Msgf("[%s] received accept from %s", n.conf.Socket.GetAddress(), pkt.Header.Source)
	id := paxosAcceptMsg.ID
	channel, ok := n.multiPaxos.paxos.mapPaxosProposeIDs[id]

	// Handle accept msg
	list, exist := n.multiPaxos.paxos.mapPaxosAcceptIDs[id]
	if !exist {
		list = make([]types.PaxosAcceptMessage, 0)
	}
	list = append(list, paxosAcceptMsg)
	n.multiPaxos.paxos.mapPaxosAcceptIDs[id] = list
	if len(list) >= n.conf.PaxosThreshold(n.conf.TotalPeers) {
		n.broadcastTLCMessageUnsafe(createBlockchainBlock(paxosAcceptMsg.Step, n.getPreviousHash(), paxosAcceptMsg.Value))
	}
	n.multiPaxos.multiPaxosMutex.Unlock()
	if ok {
		channel <- paxosAcceptMsg
	}
}

func (p *Paxos) getAcceptedValueUnsafe() (uint, *types.PaxosValue) {
	return p.acceptedID, p.acceptedValue
}

func (p *MultiPaxos) getPhaseUnsafe() uint {
	p.multiPaxosMutex.Lock()
	p.multiPaxosMutex.Unlock()
	return p.paxos.phase
}

func (p *MultiPaxos) getPhaseSafe() uint {
	return p.paxos.phase
}
func (p *MultiPaxos) setPhaseUnsafe(phase uint) {
	p.paxos.phase = phase
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

func (n *node) broadcastPaxosAcceptUnsafe(paxosProposeMsg types.PaxosProposeMessage) {

	// Create accept msg
	paxosAcceptMsg := types.PaxosAcceptMessage{
		ID:    paxosProposeMsg.ID,
		Step:  paxosProposeMsg.Step,
		Value: paxosProposeMsg.Value,
	}

	// Set the accepted value for current paxos
	n.multiPaxos.paxos.setAcceptedValueUnsafe(paxosProposeMsg.ID, paxosProposeMsg.Value)

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

func (n *node) sendPrepareMessage(step uint, id *uint) (error, int, PaxosToSend) {

	//  TODO: Change for multi-paxos
	var i uint
	retVal := PaxosToSend{}
	channel := make(chan PaxosToSend)

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
					//n.multiPaxos.setPhaseSafe(PAXOS_PHASE2)
					return nil, PROPOSER_OK, retVal
				}
				break
			case <-n.notifyEnd:
				return xerrors.Errorf("Stopping node"), PROPOSER_STOP_NODE, PaxosToSend{}

			case <-n.multiPaxos.paxos.notifyEndOfClockStep:
				return xerrors.Errorf("Stopping clock step"), PROPOSER_STOP_CLOCK, PaxosToSend{}

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
	n.multiPaxos.registerProposePaxosID(toSend.id, channel)
	receivedAccepts := make(map[string][]types.PaxosValue)

	defer func(toSend PaxosToSend, n *node, channel chan types.PaxosAcceptMessage) {
		n.multiPaxos.unregisterProposePaxosID(toSend.id)
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

		case <-n.multiPaxos.paxos.notifyEndOfClockStep:
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
	step := n.multiPaxos.getGlobalClockStep()
	// Send prepare
	for {
		log.Info().Msgf("[%s]: runConsensus: before first paxos phase", n.conf.Socket.GetAddress())
		n.multiPaxos.setPhaseSafe(PAXOS_PHASE1)
		errorPrepare, status, toSend := n.sendPrepareMessage(step, &id)
		if errorPrepare != nil {
			n.multiPaxos.setPhaseSafe(PAXOS_INIT)
			return errorPrepare, status
		}
		if status == PROPOSER_STOP_NODE || status == PROPOSER_STOP_CLOCK {
			return nil, status
		}
		n.multiPaxos.setPhaseSafe(PAXOS_PHASE2)
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
			n.multiPaxos.setPhaseSafe(PAXOS_PHASE1)
			continue
		}
		n.multiPaxos.setPhaseSafe(PAXOS_INIT)
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

func (p *MultiPaxos) notifySuccessfulConsensus() {
	p.multiPaxosMutex.Lock()
	defer p.multiPaxosMutex.Unlock()
	p.paxos.proposerRunning = false
	p.paxos.phase = PAXOS_INIT
	close(p.paxos.channelSuccCons)
	p.paxos.channelSuccCons = make(chan string)
}

func (p *MultiPaxos) isProposerRunning() (bool, chan string) {
	p.multiPaxosMutex.Lock()
	defer p.multiPaxosMutex.Unlock()
	if p.paxos.proposerRunning {
		return true, p.paxos.channelSuccCons
	}
	p.paxos.proposerRunning = true
	return false, p.paxos.notifyEndOfClockStep
	//return p.proposerRunning
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

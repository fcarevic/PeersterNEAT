package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/types"
	"sync"
)

type PaxosInfo struct {

	// Counters
	globalClockStep uint

	// Paxos instances
	paxos Paxos

	// Semaphores
	paxosInfoMutex sync.Mutex
}

type Paxos struct {

	// Counters
	clockStep uint
	maxID     uint

	// Accepted msg
	acceptedID    uint
	acceptedValue *types.PaxosValue

	// Semaphores
	paxosMutex      sync.Mutex
	acceptSemaphore sync.Mutex
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

func (p *Paxos) isExpectedPaxosProposeMsg(paxosProposeMsg types.PaxosProposeMessage) bool {
	// Acquire lock
	p.paxosMutex.Lock()
	defer p.paxosMutex.Unlock()
	return paxosProposeMsg.Step == p.clockStep && paxosProposeMsg.ID == p.maxID
}

func (p *Paxos) getAcceptedValue() (uint, *types.PaxosValue) {
	// Acquire lock
	p.acceptSemaphore.Lock()
	defer p.acceptSemaphore.Unlock()
	return p.acceptedID, p.acceptedValue
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

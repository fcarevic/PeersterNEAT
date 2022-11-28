package impl

import (
	"encoding/hex"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

type TLCInfo struct {

	// Attributes
	mapStepListTLCMsg map[uint][]types.TLCMessage
	hasBroadcasted    bool
}

func (p *MultiPaxos) nextTLCStepUnsafe() {
	// Clock step is already incremented!
	delete(p.tlc.mapStepListTLCMsg, p.globalClockStep-1)
	p.tlc.hasBroadcasted = false
}

func (p *MultiPaxos) addTLCMsgUnsafe(tlcMsg types.TLCMessage) bool {
	if tlcMsg.Step < p.globalClockStep {
		return false
	}
	list, ok := p.tlc.mapStepListTLCMsg[tlcMsg.Step]
	if !ok {
		list = make([]types.TLCMessage, 0)
	}
	list = append(list, tlcMsg)
	p.tlc.mapStepListTLCMsg[tlcMsg.Step] = list
	log.Info().Msgf("added tlc for step %d in current step %d", tlcMsg.Step, p.globalClockStep)
	return true
}

func (p *MultiPaxos) getNumberOfTLCMessagesForCurrentUnsafe() int {
	list, ok := p.tlc.mapStepListTLCMsg[p.globalClockStep]
	if !ok {
		return 0
	}
	return len(list)
}

func (p *MultiPaxos) getFirstBlockForCurrentStepUnsafe() (types.BlockchainBlock, error) {
	list, ok := p.tlc.mapStepListTLCMsg[p.globalClockStep]
	if !ok {
		return types.BlockchainBlock{}, xerrors.Errorf("No block")
	}
	if len(list) == 0 {
		return types.BlockchainBlock{}, xerrors.Errorf("List empty, no block")
	}
	return list[0].Block, nil
}

func (n *node) isTLCConsensusReachedUnsafe() bool {
	log.Info().Msgf("[%s] num: %d, thresh: %d , steps: %d", n.conf.Socket.GetAddress(),
		n.multiPaxos.getNumberOfTLCMessagesForCurrentUnsafe(),
		n.conf.PaxosThreshold(n.conf.TotalPeers),
		n.multiPaxos.globalClockStep)
	return n.multiPaxos.getNumberOfTLCMessagesForCurrentUnsafe() >= n.conf.PaxosThreshold(n.conf.TotalPeers)
}

func (n *node) handleTlCMsg(tlcMsg types.TLCMessage) {

	// Acquire lock
	n.multiPaxos.multiPaxosMutex.Lock()
	defer n.multiPaxos.multiPaxosMutex.Unlock()
	// Add message
	if !n.multiPaxos.addTLCMsgUnsafe(tlcMsg) {
		return
	}
	log.Info().Msgf("[%s] TLC Message added", n.conf.Socket.GetAddress())

	//check if consensus reached
	if n.isTLCConsensusReachedUnsafe() {
		log.Info().Msgf("[%s] TLC consensus reached", n.conf.Socket.GetAddress())
		block, err := n.multiPaxos.getFirstBlockForCurrentStepUnsafe()
		if err != nil {
			return
		}
		errSave := n.saveInBlockchainUnsafe(block)
		if errSave != nil {
			return
		}
		n.broadcastTLCMessageUnsafe(block)
		n.multiPaxos.nextGlobalClockStepUnsafe()
		n.tlcCatchUpUnsafe()
	}
}

func (n *node) saveInBlockchainUnsafe(block types.BlockchainBlock) error {
	// Get block bytes
	blockBytes, err := block.Marshal()
	if err != nil {
		return err
	}

	hashEnc := hex.EncodeToString(block.Hash)
	n.conf.Storage.GetBlockchainStore().Set(hashEnc, blockBytes)
	n.conf.Storage.GetBlockchainStore().Set(storage.LastBlockKey, block.Hash)
	n.conf.Storage.GetNamingStore().Set(block.Value.Filename, []byte(block.Value.Metahash))
	log.Info().Msgf("[%s] saved in the blockchain: fname: %s  key: %s",
		n.conf.Socket.GetAddress(), block.Value.Filename, hashEnc)
	return nil
}
func (n *node) tlcCatchUpUnsafe() {
	for n.isTLCConsensusReachedUnsafe() {
		block, err := n.multiPaxos.getFirstBlockForCurrentStepUnsafe()
		if err != nil {
			return
		}
		errSave := n.saveInBlockchainUnsafe(block)
		if errSave != nil {
			return
		}
		n.multiPaxos.nextGlobalClockStepUnsafe()
	}
	log.Info().Msgf("[%s] exiting tlc catchup", n.conf.Socket.GetAddress())
}
func (n *node) broadcastTLCMessageUnsafe(block types.BlockchainBlock) {
	if n.multiPaxos.tlc.hasBroadcasted || block.Index != n.multiPaxos.globalClockStep {
		return
	}
	n.multiPaxos.tlc.hasBroadcasted = true

	// Create msg
	tlcMsg := types.TLCMessage{
		Step:  n.multiPaxos.globalClockStep,
		Block: block,
	}

	// Marshall msg
	transportMsg, errMarshall := n.conf.MessageRegistry.MarshalMessage(tlcMsg)
	if errMarshall != nil {
		log.Error().Msgf("[%s]broadcastTLCMessageUnsafe : Marshall Failed, %s",
			n.conf.Socket.GetAddress(), errMarshall.Error())
		return
	}

	log.Info().Msgf("[%s]: Prepare to broadcast tlc : step: %d", n.conf.Socket.GetAddress(), tlcMsg.Step)
	// Broadcast
	err := n.Broadcast(transportMsg)
	if err != nil {
		log.Error().Msgf("[%s]broadcastTLCMessageUnsafe: Broadcast failed: %s",
			n.conf.Socket.GetAddress(),
			err.Error())
		return
	}
	log.Info().Msgf("[%s]: Broadcasted tlc : step: %d", n.conf.Socket.GetAddress(), tlcMsg.Step)
}

//func (n *node) broadcastTLCMessage(block types.BlockchainBlock) {
//	n.multiPaxos.multiPaxosMutex.Lock()
//	defer n.multiPaxos.multiPaxosMutex.Unlock()
//	n.broadcastTLCMessageUnsafe(block)
//}

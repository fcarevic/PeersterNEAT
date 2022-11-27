package impl

import (
	"encoding/hex"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"sync"
)

type TLCInfo struct {

	// Attributes
	globalClockStep   uint
	mapStepListTLCMsg map[uint][]types.TLCMessage
	hasBroadcasted    bool

	// Semaphores
	tlcMutex sync.Mutex
}

func (t *TLCInfo) nextStepUnsafe() {
	delete(t.mapStepListTLCMsg, t.globalClockStep)
	t.globalClockStep += 1
	t.hasBroadcasted = false

}

func (n *node) nextTLCStep() {
	n.tlcInfo.nextStepUnsafe()
	n.paxosInfo.incrementGlobalClockStep()
}

func (t *TLCInfo) addTLCMsgUnsafe(tlcMsg types.TLCMessage) bool {
	if tlcMsg.Step < t.globalClockStep {
		return false
	}
	list, ok := t.mapStepListTLCMsg[tlcMsg.Step]
	if !ok {
		list = make([]types.TLCMessage, 0)
	}
	list = append(list, tlcMsg)
	t.mapStepListTLCMsg[tlcMsg.Step] = list
	return true
}

func (t *TLCInfo) getNumberOfTLCMessagesForCurrentUnsafe() int {
	list, ok := t.mapStepListTLCMsg[t.globalClockStep]
	if !ok {
		return 0
	}
	return len(list)
}

func (t *TLCInfo) getFirstBlockForCurrentStepUnsafe() (error, types.BlockchainBlock) {
	list, ok := t.mapStepListTLCMsg[t.globalClockStep]
	if !ok {
		return xerrors.Errorf("No block"), types.BlockchainBlock{}
	}
	if len(list) == 0 {
		return xerrors.Errorf("List empty, no block"), types.BlockchainBlock{}
	}
	return nil, list[0].Block
}

func (n *node) isTLCConsensusReachedUnsafe() bool {
	log.Info().Msgf("[%s] num: %d, thresh: %d , steps: %d %d", n.conf.Socket.GetAddress(),
		n.tlcInfo.getNumberOfTLCMessagesForCurrentUnsafe(),
		n.conf.PaxosThreshold(n.conf.TotalPeers),
		n.tlcInfo.globalClockStep, n.paxosInfo.getGlobalClockStep())
	return n.tlcInfo.getNumberOfTLCMessagesForCurrentUnsafe() >= n.conf.PaxosThreshold(n.conf.TotalPeers) &&
		n.tlcInfo.globalClockStep == n.paxosInfo.getGlobalClockStep()
}

func (n *node) handleTlCMsg(tlcMsg types.TLCMessage) {

	// Acquire lock
	n.tlcInfo.tlcMutex.Lock()
	defer n.tlcInfo.tlcMutex.Unlock()
	// Add message
	if !n.tlcInfo.addTLCMsgUnsafe(tlcMsg) {
		return
	}
	log.Info().Msgf("[%s] TLC Message added", n.conf.Socket.GetAddress())

	//check if consensus reached
	if n.isTLCConsensusReachedUnsafe() {
		log.Info().Msgf("[%s] TLC consensus reached", n.conf.Socket.GetAddress())
		err, block := n.tlcInfo.getFirstBlockForCurrentStepUnsafe()
		if err != nil {
			return
		}
		errSave := n.saveInBlockchainUnsafe(block)
		if errSave != nil {
			return
		}
		n.broadcastTLCMessageUnsafe(block)
		n.nextTLCStep()
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
		err, block := n.tlcInfo.getFirstBlockForCurrentStepUnsafe()
		if err != nil {
			return
		}
		errSave := n.saveInBlockchainUnsafe(block)
		if errSave != nil {
			return
		}
		n.nextTLCStep()
	}
}
func (n *node) broadcastTLCMessageUnsafe(block types.BlockchainBlock) {
	if n.tlcInfo.hasBroadcasted || block.Index != n.tlcInfo.globalClockStep {
		return
	}
	n.tlcInfo.hasBroadcasted = true

	// Create msg
	tlcMsg := types.TLCMessage{
		Step:  n.tlcInfo.globalClockStep,
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

func (n *node) broadcastTLCMessage(block types.BlockchainBlock) {

	n.tlcInfo.tlcMutex.Lock()
	defer n.tlcInfo.tlcMutex.Unlock()
	n.broadcastTLCMessageUnsafe(block)
}

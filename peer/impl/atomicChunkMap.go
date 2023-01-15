package impl

import (
	"bytes"
	"go.dedis.ch/cs438/types"
	"sync"
)

type AtomicChunkMap struct {
	chunkMap       map[string][][]byte
	receivedChunks map[string]int
	mutex          sync.RWMutex
}

func NewAtomicChunkMap() *AtomicChunkMap {
	return &AtomicChunkMap{
		chunkMap:       make(map[string][][]byte),
		receivedChunks: make(map[string]int),
		mutex:          sync.RWMutex{},
	}
}

func (atomicCM *AtomicChunkMap) Add(msg *types.CrowdsDownloadReplyMessage) bool {
	atomicCM.mutex.Lock()
	defer atomicCM.mutex.Unlock()

	_, exists := atomicCM.chunkMap[msg.RequestID]
	if !exists {
		atomicCM.chunkMap[msg.RequestID] = make([][]byte, msg.TotalChunks)
	}

	atomicCM.chunkMap[msg.RequestID][msg.Index] = msg.Value
	atomicCM.receivedChunks[msg.RequestID]++

	return atomicCM.receivedChunks[msg.RequestID] == msg.TotalChunks
}

func (atomicCM *AtomicChunkMap) GetFile(fileID string) []byte {
	atomicCM.mutex.Lock()
	defer atomicCM.mutex.Unlock()

	chunks, exists := atomicCM.chunkMap[fileID]
	if !exists {
		return nil
	}

	return bytes.Join(chunks, []byte(""))
}

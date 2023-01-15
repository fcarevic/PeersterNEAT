package impl

import "sync"

// AtomicChannelTable implements a thread-safe map for storing channels.
type AtomicChannelTable struct {
	channelMap map[string]chan string
	mutex      sync.Mutex
}

// NewAtomicChannelTable creates a new thread-safe map for storing channels.
func NewAtomicChannelTable() *AtomicChannelTable {
	return &AtomicChannelTable{
		channelMap: make(map[string]chan string),
	}
}

// StoreChannel stores channel under a specific key.
func (atomicCT *AtomicChannelTable) StoreChannel(key string, channel chan string) {
	atomicCT.mutex.Lock()
	defer atomicCT.mutex.Unlock()

	atomicCT.channelMap[key] = channel
}

// DeleteChannels deletes channels based on given keys input.
func (atomicCT *AtomicChannelTable) DeleteChannels(keys ...string) {
	atomicCT.mutex.Lock()
	defer atomicCT.mutex.Unlock()

	for _, key := range keys {
		delete(atomicCT.channelMap, key)
	}
}

// CloseDelete closes and deletes channel based on given key.
func (atomicCT *AtomicChannelTable) CloseDelete(key string) bool {
	atomicCT.mutex.Lock()
	defer atomicCT.mutex.Unlock()

	channel, ok := atomicCT.channelMap[key]
	if ok {
		close(channel)
		delete(atomicCT.channelMap, key)
	}

	return ok
}

// CloseDeleteAll closes and deletes all channels.
func (atomicCT *AtomicChannelTable) CloseDeleteAll() {
	atomicCT.mutex.Lock()
	defer atomicCT.mutex.Unlock()

	for key, channel := range atomicCT.channelMap {
		close(channel)
		delete(atomicCT.channelMap, key)
	}
}

// NotifySendDelete sends value into channel given by key and deletes it from map.
func (atomicCT *AtomicChannelTable) NotifySendDelete(key string, value string) bool {
	atomicCT.mutex.Lock()
	defer atomicCT.mutex.Unlock()

	channel, ok := atomicCT.channelMap[key]
	if ok {
		delete(atomicCT.channelMap, key)
		channel <- value
	}

	return ok
}

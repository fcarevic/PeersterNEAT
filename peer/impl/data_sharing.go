package impl

import (
	"crypto"
	"encoding/hex"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"io"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type DataSharing struct {
	catalog         peer.Catalog
	dataRequestsMap map[string]chan []byte

	// Semaphores
	catalogMutex        sync.Mutex
	dataRequestMapMutex sync.Mutex
}

// GetCatalog returns the peer's catalog.
func (n *node) GetCatalog() peer.Catalog {

	// Acquire lock
	n.dataSharing.catalogMutex.Lock()
	defer n.dataSharing.catalogMutex.Unlock()

	// Copy
	catalogCopy := make(peer.Catalog)
	for dataKey, peerMap := range n.dataSharing.catalog {
		peerMapCopy := make(map[string]struct{})
		for peerAddress, v := range peerMap {
			peerMapCopy[peerAddress] = v
		}
		catalogCopy[dataKey] = peerMapCopy
	}
	return catalogCopy
}

// UpdateCatalog tells the peer about a piece of data referenced by 'key'
// being available on other peers. It should update the peer's catalog.
func (n *node) UpdateCatalog(key string, peer string) {

	// Don't allow to store self in the map
	if peer == n.conf.Socket.GetAddress() {
		return
	}
	// Acquire lock
	n.dataSharing.catalogMutex.Lock()
	defer n.dataSharing.catalogMutex.Unlock()

	// Add to catalog
	peerMap, ok := n.dataSharing.catalog[key]
	if !ok {
		peerMap = make(map[string]struct{})
	}
	peerMap[peer] = struct{}{}
	n.dataSharing.catalog[key] = peerMap
}

// Upload stores a new data blob on the peer and will make it available to
// other peers. The blob will be split into chunks.
func (n *node) Upload(data io.Reader) (metahash string, err error) {
	// Initialize
	var metafileValue string
	var metafileKeyParts []byte
	//var addedChunks []string

	// Read chunk by chunk
	for {
		chunk := make([]byte, n.conf.ChunkSize)
		numBytes, errRead := data.Read(chunk)
		if errRead != nil && errRead != io.EOF {
			log.Error().Msgf("[%s]: Upload: error while reading the file: %s",
				n.conf.Socket.GetAddress(),
				errRead.Error())

			// Delete saved chunks
			//for _, chunkKey := range addedChunks {
			//	n.conf.Storage.GetDataBlobStore().Delete(chunkKey)
			//}
			return "", errRead
		}

		if numBytes > 0 {

			// Add this chunk to metafile values
			if len(metafileValue) != 0 {
				metafileValue = metafileValue + peer.MetafileSep
			}
			metafileValue = metafileValue + hex.EncodeToString(hashSHA256(chunk[:numBytes]))

			// Add this chunk to storage
			hash := hashSHA256(chunk[:numBytes])
			metafileKeyParts = append(metafileKeyParts, hash...)
			key := hex.EncodeToString(hash)
			n.conf.Storage.GetDataBlobStore().Set(key, chunk[:numBytes])
			//addedChunks = append(addedChunks, key)
		}

		// Didn't finish reading
		if errRead != io.EOF {
			continue
		}

		// If EOF, add to storage
		metafileKey := hex.EncodeToString(hashSHA256(metafileKeyParts))
		n.conf.Storage.GetDataBlobStore().Set(metafileKey, []byte(metafileValue))
		return metafileKey, nil
	}
}

func (n *node) Download(metahash string) ([]byte, error) {

	// Get metafile
	metafileValueBytes, err := n.getValueForMetahash(metahash)
	if err != nil {
		return nil, err
	}

	// Extract parts of the file
	fileParts := strings.Split(string(metafileValueBytes), peer.MetafileSep)
	var mapOfParts = make(map[string][]byte)
	var file []byte
	for _, key := range fileParts {

		// Get value locally or remotely
		value, errValue := n.getValueForMetahash(key)
		if errValue != nil {
			return nil, errValue
		}
		// Append to file
		file = append(file, value...)

		// Add to map
		mapOfParts[key] = value
	}

	// Store locally if successful
	for k, v := range mapOfParts {
		n.conf.Storage.GetDataBlobStore().Set(k, v)
	}
	return file, nil
}

func (n *node) getValueForMetahash(metahash string) ([]byte, error) {

	// Load from storage if exist
	localValue := n.conf.Storage.GetDataBlobStore().Get(metahash)
	if len(localValue) != 0 {
		return localValue, nil
	}

	// Get from remote peer
	var peerAddress, err = n.getRandomPeerForDownload(metahash)
	if err != nil {
		return nil, err
	}

	// Create DataRequestMessage
	msg := types.DataRequestMessage{
		RequestID: xid.New().String(),
		Key:       metahash,
	}

	// Convert to transport.Message
	transportMessage, errCast := n.conf.MessageRegistry.MarshalMessage(msg)
	if errCast != nil {
		return nil, errCast
	}
	// Register msg
	var channel = make(chan []byte)
	n.registerDataRequestMessage(msg, channel)
	defer n.unregisterDataRequestMessage(msg)

	// Initialize backoff mechanism
	var duration = n.conf.BackoffDataRequest.Initial
	var i uint = 0
	for i < n.conf.BackoffDataRequest.Retry {
		// Send msg
		errSend := n.Unicast(peerAddress, transportMessage)
		if errSend != nil {
			return nil, errSend
		}

		// Wait for timeout
		select {
		case bytes := <-channel:
			if bytes == nil {
				return nil, xerrors.Errorf("Nil bytes received")
			} else {
				return bytes, nil
			}

		case <-time.After(duration):
			duration = duration * time.Duration(
				n.conf.BackoffDataRequest.Factor)
			break
		}

		i = i + 1
	}
	return nil, xerrors.Errorf("Failed to send")
}

func (n *node) getRandomPeerForDownload(metahash string) (string, error) {

	// Get catalog
	catalog := n.GetCatalog()

	// Check if exist peers for the metahash
	var peerMap, ok = catalog[metahash]
	if !ok {
		return "", xerrors.Errorf("No information for random download peer.")
	}

	// Get random peer
	var peers []string
	for k := range peerMap {
		peers = append(peers, k)
	}
	return peers[rand.Intn(len(peers))], nil
}

func (n *node) registerDataRequestMessage(msg types.DataRequestMessage, channel chan []byte) {

	// Acquire lock
	n.dataSharing.dataRequestMapMutex.Lock()
	defer n.dataSharing.dataRequestMapMutex.Unlock()

	// Add to map
	n.dataSharing.dataRequestsMap[msg.RequestID] = channel

}
func (n *node) unregisterDataRequestMessage(msg types.DataRequestMessage) {
	// Acquire lock
	n.dataSharing.dataRequestMapMutex.Lock()
	defer n.dataSharing.dataRequestMapMutex.Unlock()

	// Delete from map and close channel
	channel, ok := n.dataSharing.dataRequestsMap[msg.RequestID]
	if ok {
		close(channel)
		delete(n.dataSharing.dataRequestsMap, msg.RequestID)
	}
}

func (n *node) processDataReply(replyMsg types.DataReplyMessage) {

	// Acquire lock
	n.dataSharing.dataRequestMapMutex.Lock()
	defer n.dataSharing.dataRequestMapMutex.Unlock()

	// Send to map if exist
	channel, ok := n.dataSharing.dataRequestsMap[replyMsg.RequestID]
	if ok {
		channel <- replyMsg.Value
	}
	return
}

/// DELETE
//// DataSharing describes functions to share data in a bittorrent-like system.
//type DataSharing interface {

//
//	// Download will get all the necessary chunks corresponding to the given
//	// metahash that references a blob, and return a reconstructed blob. The
//	// peer will save locally the chunks that it doesn't have for further
//	// sharing. Returns an error if it can't get the necessary chunks.
//	//
//	// - Implemented in HW2
//	Download(metahash string) ([]byte, error)
//
//	// Tag creates a mapping between a (file)name and a metahash.
//	//
//	// - Implemented in HW2
//	// - Improved in HW3: ensure uniqueness with blockchain/TLC/Paxos
//	Tag(name string, mh string) error
//
//	// Resolve returns the corresponding metahash of a given (file)name. Returns
//	// an empty string if not found.
//	//
//	// - Implemented in HW2
//	Resolve(name string) (metahash string)
//
//	// SearchAll returns all the names that exist matching the given regex. It
//	// merges results from the local storage and from the search request reply
//	// sent to a random neighbor using the provided budget. It makes the peer
//	// update its catalog and name storage according to the SearchReplyMessages
//	// received. Returns an empty result if nothing found. An error is returned
//	// in case of an exceptional event.
//	//
//	// - Implemented in HW2
//	SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) (names []string, err error)
//
//	// SearchFirst uses an expanding ring configuration and returns a name as
//	// soon as it finds a peer that "fully matches" a data blob. It makes the
//	// peer update its catalog and name storage according to the
//	// SearchReplyMessages received. Returns an empty string if nothing was
//	// found.
//	SearchFirst(pattern regexp.Regexp, conf ExpandingRing) (name string, err error)
//}
//

//// ExpandingRing defines an expanding ring configuration.
//type ExpandingRing struct {
//	// Initial budget. Should be at least 1.
//	Initial uint
//
//	// Budget is multiplied by factor after each try
//	Factor uint
//
//	// Number of times to try. A value of 1 means there will be only 1 attempt.
//	Retry uint
//
//	// Timeout before retrying when no response received.
//	Timeout time.Duration
//}

// Hash data with SHA256
func hashSHA256(data []byte) []byte {
	sha256 := crypto.SHA256.New()
	sha256.Write(data)
	hash := sha256.Sum(nil)
	return hash
}

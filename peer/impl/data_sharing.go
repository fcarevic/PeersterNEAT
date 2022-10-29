package impl

import (
	"crypto"
	"encoding/hex"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"io"
	"sync"
)

type DataSharing struct {
	catalog peer.Catalog

	// Semaphores
	catalogMutex sync.Mutex
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
		for peer, v := range peerMap {
			peerMapCopy[peer] = v
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
		if errRead != nil && err != io.EOF {
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
			metafileValue = metafileValue + hex.EncodeToString(hashSHA256(chunk))

			// Add this chunk to storage
			hash := hashSHA256(chunk)
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
		metafileKey := hex.EncodeToString(metafileKeyParts)
		n.conf.Storage.GetDataBlobStore().Set(metafileKey, []byte(metafileValue))
		return metafileKey, nil
	}
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

package impl

import (
	"crypto"
	"encoding/hex"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"io"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"
)

type DataSharing struct {
	catalog                    peer.Catalog
	dataRequestsMap            map[string]chan []byte
	receivedRequests           map[string]bool
	remoteFullyKnownMetahashes map[string]chan string

	// Semaphores
	catalogMutex                    sync.Mutex
	dataRequestMapMutex             sync.Mutex
	receivedRequestsMutex           sync.Mutex
	remoteFullyKnownMetahashesMutex sync.Mutex
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

func (n *node) removeFromCatalog(src string, metahash string) {
	// Acquire lock
	n.dataSharing.catalogMutex.Lock()
	defer n.dataSharing.catalogMutex.Unlock()

	// Add to catalog
	peerMap, ok := n.dataSharing.catalog[metahash]
	if !ok {
		return
	}
	delete(peerMap, src)
	n.dataSharing.catalog[metahash] = peerMap

}

// Upload stores a new data blob on the peer and will make it available to
// other peers. The blob will be split into chunks.
func (n *node) Upload(data io.Reader) (metahash string, err error) {

	if n.conf.ChunkSize == 0 {
		return "", xerrors.Errorf("Chunk size cannot be zero")
	}

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
		if uint(len([]byte(metafileValue))) > n.conf.ChunkSize {
			return "", xerrors.Errorf("Metafile is larger than 1 chunk")
		}
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

	if uint(len(metafileValueBytes)) > n.conf.ChunkSize {
		return nil, xerrors.Errorf("Metafile is larger than 1 chunk")
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
	var i uint
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
			}
			return bytes, nil
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

func (n *node) processDataReply(replyMsg types.DataReplyMessage, pkt transport.Packet) {

	// Delete if empty
	if replyMsg.Value == nil {
		n.removeFromCatalog(pkt.Header.Source, replyMsg.Key)
	}

	// Acquire lock
	n.dataSharing.dataRequestMapMutex.Lock()
	defer n.dataSharing.dataRequestMapMutex.Unlock()

	// Send to map if exist
	channel, ok := n.dataSharing.dataRequestsMap[replyMsg.RequestID]
	if ok {
		channel <- replyMsg.Value
	}
}

// Tag creates a mapping between a (file)name and a metahash.
func (n *node) Tag(name string, mh string) error {

	if n.conf.TotalPeers > 1 {
		consensusReached := false
		for !consensusReached {
			if n.conf.Storage.GetNamingStore().Get(name) != nil {
				return xerrors.Errorf("Name already exist: %s", name)
			}

			// Wait for the turn
			running, channel := n.multiPaxos.isProposerRunning()
			if running {
				select {
				case <-n.notifyEnd:
					return nil

					// This chaannel is waiting the end of previous proposer
				case <-channel:
					continue
				}
			}

			status, errCons := n.runConsensus(types.PaxosValue{
				UniqID:   xid.New().String(),
				Metahash: mh,
				Filename: name,
			})

			// Wait for the end of paxos clock
			<-channel

			switch status {
			case ProposerStopNode:
				n.multiPaxos.notifySuccessfulConsensus()
				return nil
			case ProposerError:
				n.multiPaxos.notifySuccessfulConsensus()
				return errCons
			case ProposerOurValue:
				n.multiPaxos.notifySuccessfulConsensus()
				consensusReached = true
			}
		}
		return nil
	}
	n.conf.Storage.GetNamingStore().Set(name, []byte(mh))
	return nil

}

// Resolve returns the corresponding metahash of a given (file)name. Returns
// an empty string if not found.
func (n *node) Resolve(name string) (metahash string) {
	bytes := n.conf.Storage.GetNamingStore().Get(name)
	if bytes == nil {
		return ""
	}
	return string(bytes)
}

// SearchAll returns all the names that exist matching the given regex. It
// merges results from the local storage and from the search request reply
// sent to a random neighbor using the provided budget. It makes the peer
// update its catalog and name storage according to the SearchReplyMessages
// received. Returns an empty result if nothing found. An error is returned
// in case of an exceptional event.
//
// - Implemented in HW2
func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) (names []string, err error) {

	// Get request ID
	id := xid.New().String()

	// Send search to random nodes
	if n.sendSearchRequestRandomly(budget, id, reg) {
		// Sleep and wait for the replies to be handled
		time.Sleep(timeout)
	}

	// Get matching names from local storage
	localNames := n.getFilenamesFromLocalStorage()
	return localNames, nil
}

func (n *node) getFilenamesFromLocalStorage() []string {
	var localNames []string
	n.conf.Storage.GetNamingStore().ForEach(func(key string, val []byte) bool {
		localNames = append(localNames, key)
		return true
	})
	return localNames
}

func (n *node) getHashesOfChunksForFile(metahash string) [][]byte {
	metafileValue := n.conf.Storage.GetDataBlobStore().Get(metahash)
	if metafileValue == nil {
		return nil
	}
	chunks := strings.Split(string(metafileValue), peer.MetafileSep)
	var fileChunks [][]byte
	for _, chunkTag := range chunks {
		chunk := n.conf.Storage.GetDataBlobStore().Get(chunkTag)
		if chunk != nil {
			chunk = []byte(chunkTag)
		}
		fileChunks = append(fileChunks, chunk)
	}
	return fileChunks
}

func (n *node) checkDuplicateAndRegister(requestID string) bool {
	// Acquire lock
	n.dataSharing.receivedRequestsMutex.Lock()
	defer n.dataSharing.receivedRequestsMutex.Unlock()
	_, ok := n.dataSharing.receivedRequests[requestID]
	if !ok {
		n.dataSharing.receivedRequests[requestID] = true
	}
	return ok
}

// SearchFirst uses an expanding ring configuration and returns a name as
// soon as it finds a peer that "fully matches" a data blob. It makes the
// peer update its catalog and name storage according to the
// SearchReplyMessages received. Returns an empty string if nothing was
// found.
func (n *node) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (name string, err error) {

	// Check if fully known
	var localNames = n.getFilenamesFromLocalStorage()
	for _, localName := range localNames {
		matched := pattern.MatchString(localName)
		if matched {
			if n.checkIfFullyKnownLocally(localName) {
				return localName, nil
			}
		}
	}

	channel := make(chan string)
	var listIds []string

	defer func() {
		n.unregisterFullyKnown(listIds)
		close(channel)
	}()

	initialBudget := conf.Initial
	var cnt uint
	for cnt < conf.Retry {

		// Search remote peers
		id := xid.New().String()
		listIds = append(listIds, id)
		n.registerFullyKnown(id, channel)

		// Send request search
		if !n.sendSearchRequestRandomly(initialBudget, id, pattern) {
			log.Info().Msgf(
				"[%s]:sendSearchRequestRandomly: false",
				n.conf.Socket.GetAddress())
			return "", nil
		}

		// Wait for the response
		select {
		case filename := <-channel:
			return filename, err
		case <-time.After(conf.Timeout):
			break
		}
		// Update budget and go again
		initialBudget = initialBudget * conf.Factor
		cnt++
	}

	// Check if fully known
	localNames = n.getFilenamesFromLocalStorage()
	for _, localName := range localNames {
		matched := pattern.MatchString(localName)
		if matched {
			if n.checkIfFullyKnownLocally(localName) {
				return localName, nil
			}
		}
	}

	return "", nil
}

func (n *node) sendSearchRequestRandomly(budget uint, id string, pattern regexp.Regexp) bool {
	// Get matching names from remote peers
	// Get matching names from remote peers
	var peers []string
	var counter uint
	for counter < budget {
		peerAddress, errNeigh := n.getRangomNeighbour(peers)
		if errNeigh != nil || peerAddress == "" {
			break
		}

		peers = append(peers, peerAddress)
		counter++
	}
	log.Info().Msgf(
		"[%s]:sendSearchRequestRandomly: sneds to peeers: %s",
		n.conf.Socket.GetAddress(),
		peers)
	if len(peers) == 0 {
		return false
	}

	n.checkDuplicateAndRegister(id)

	for ind, peerAddress := range peers {

		// Calculate peer budget
		peerBudget := budget / uint(len(peers))
		reminder := budget % uint(len(peers))
		if uint(ind) < reminder {
			peerBudget++
		}

		// Craft search request
		searchRequest := types.SearchRequestMessage{
			RequestID: id,
			Origin:    n.conf.Socket.GetAddress(),
			Pattern:   pattern.String(),
			Budget:    peerBudget,
		}

		// Serialize request
		transportMsg, errCast := n.conf.MessageRegistry.MarshalMessage(searchRequest)
		if errCast != nil {
			log.Error().Msgf("[%s] SearchAll:Failed to marshall the message : %s",
				n.conf.Socket.GetAddress(), errCast.Error())
		}

		// Send request
		errSend := n.Unicast(peerAddress, transportMsg)
		if errSend != nil {
			log.Error().Msgf("[%s] SearchAll:Failed to send the message : %s",
				n.conf.Socket.GetAddress(), errSend.Error())
		}
	}
	return true
}
func (n *node) notifyFullyKnown(requestID string, filename string) {
	defer func() {
		if err := recover(); err != nil {
			log.Error().Msgf("[%s]: notifyFullyKnown: Panic on sending on the channel",
				n.conf.Socket.GetAddress())
		}
	}()
	//Acquire lock
	n.dataSharing.remoteFullyKnownMetahashesMutex.Lock()
	channel, ok := n.dataSharing.remoteFullyKnownMetahashes[requestID]
	n.dataSharing.remoteFullyKnownMetahashesMutex.Unlock()
	log.Info().Msgf(
		"[%s] received fully known %s",
		n.conf.Socket.GetAddress(),
		filename)
	if ok {
		channel <- filename
	}
}
func (n *node) checkIfFullyKnownLocally(name string) bool {
	// Check locally
	metahashBytes := n.conf.Storage.GetNamingStore().Get(name)
	if metahashBytes == nil {
		return false
	}
	metahash := string(metahashBytes)
	chunks := n.getHashesOfChunksForFile(metahash)
	if chunks != nil {
		fullyKnown := true
		for _, chunk := range chunks {
			if chunk == nil {
				fullyKnown = false
			}
			if fullyKnown {
				return true
			}
		}
	}
	return false
}
func (n *node) registerFullyKnown(requestID string, channel chan string) {
	// Acquire lock
	n.dataSharing.remoteFullyKnownMetahashesMutex.Lock()
	defer n.dataSharing.remoteFullyKnownMetahashesMutex.Unlock()
	n.dataSharing.remoteFullyKnownMetahashes[requestID] = channel
}

func (n *node) unregisterFullyKnown(requestsID []string) {
	// Acquire lock
	n.dataSharing.remoteFullyKnownMetahashesMutex.Lock()
	defer n.dataSharing.remoteFullyKnownMetahashesMutex.Unlock()
	for _, requestID := range requestsID {
		_, ok := n.dataSharing.remoteFullyKnownMetahashes[requestID]
		if ok {
			delete(n.dataSharing.remoteFullyKnownMetahashes, requestID)
		}
	}
}

// Hash data with SHA256
func hashSHA256(data []byte) []byte {
	sha256 := crypto.SHA256.New()
	sha256.Write(data)
	hash := sha256.Sum(nil)
	return hash
}

func (n *node) handleSearchRequestLocally(searchRequestMsg types.SearchRequestMessage, pkt transport.Packet) {

	// Get filename by regex
	localNames := n.getFilenamesFromLocalStorage()
	var fileInfos []types.FileInfo
	for _, name := range localNames {
		matched, errMatch := regexp.MatchString(searchRequestMsg.Pattern, name)

		if errMatch != nil {
			continue
		}
		if matched {
			metahash := string(n.conf.Storage.GetNamingStore().Get(name))
			chunks := n.getHashesOfChunksForFile(metahash)
			if chunks != nil {
				fileInfo := types.FileInfo{
					Name:     name,
					Metahash: metahash,
					Chunks:   chunks,
				}
				fileInfos = append(fileInfos, fileInfo)
			}
		}
	}

	// Create reply msg
	replyMsg := types.SearchReplyMessage{
		RequestID: searchRequestMsg.RequestID,
		Responses: fileInfos,
	}
	// Convert to packet
	packet, errPkt := n.msgTypesToPacket(
		n.conf.Socket.GetAddress(),
		n.conf.Socket.GetAddress(),
		searchRequestMsg.Origin,
		replyMsg)
	if errPkt != nil {
		log.Error().Msgf("[%s] searchRequestMessageCallback: Unable to craft packet: %s",
			n.conf.Socket.GetAddress(), errPkt.Error())
	}
	// Send packet
	errSend := n.conf.Socket.Send(pkt.Header.Source, packet, TIMEOUT)
	if errSend != nil {
		log.Error().Msgf("[%s] searchRequestMessageCallback: Unable to send packet: %s",
			n.conf.Socket.GetAddress(), errPkt.Error())
	}

}

func (n *node) forwardRequestToNeighbours(budget uint, searchRequestMsg types.SearchRequestMessage,
	pkt transport.Packet) {
	var peers []string
	var excludedPeers = []string{searchRequestMsg.Origin, pkt.Header.RelayedBy}
	for uint(len(peers)) < budget {
		peerAddress, errNeigh := n.getRangomNeighbour(excludedPeers)
		if errNeigh != nil || peerAddress == "" {
			break
		}
		peers = append(peers, peerAddress)
		excludedPeers = append(excludedPeers, peerAddress)
	}

	for ind, peerAddress := range peers {
		var peerBudget = budget / uint(len(peers))
		if uint(ind) < budget%uint(len(peers)) {
			peerBudget++
		}
		var msg = types.SearchRequestMessage{
			RequestID: searchRequestMsg.RequestID,
			Budget:    peerBudget,
			Pattern:   searchRequestMsg.Pattern,
			Origin:    searchRequestMsg.Origin,
		}
		transportMsg, errCast := n.conf.MessageRegistry.MarshalMessage(msg)
		if errCast != nil {
			log.Error().Msgf("[%s] searchRequestMessageCallback: Unable to marshall msg: %s",
				n.conf.Socket.GetAddress(), errCast.Error())
		}
		log.Info().Msgf("[%s] Relay search from %s req to %s",
			n.conf.Socket.GetAddress(), searchRequestMsg.Origin, peerAddress)
		errSend := n.Unicast(peerAddress, transportMsg)
		if errSend != nil {
			log.Error().Msgf("[%s] searchRequestMessageCallback: Unable to send unicast: %s",
				n.conf.Socket.GetAddress(), errSend.Error())
		}

	}

}

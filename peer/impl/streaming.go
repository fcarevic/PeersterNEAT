package impl

import (
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"io"
	"sync"
	"time"
)

const MULTICASTCHUNKSIZE = 512
const STREAMSLEEPTIME = time.Millisecond

type StreamInfo struct {
	// Attributes
	mapClients   map[string][]string
	mapListening map[string][]types.StreamMessage

	// Encryption
	mapKeysListening map[string][]byte

	// Semaphores
	streamInfoMutex sync.Mutex
}

func (s *StreamInfo) registerListening(streamID string, key []byte) error {
	// Acquire lock
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()
	_, ok := s.mapListening[streamID]
	if ok {
		return xerrors.Errorf("Stream already exists")
	}
	s.mapListening[streamID] = make([]types.StreamMessage, 0)
	s.mapKeysListening[streamID] = key
	return nil
}

func (s *StreamInfo) addListeningStreamMessageUnsafe(streamID string, msg types.StreamMessage) error {
	streamMsgs, ok := s.mapListening[streamID]
	if !ok {
		return xerrors.Errorf("Stream does not exist.")
	}
	streamMsgs = append(streamMsgs, msg)
	s.mapListening[streamID] = sortStreamMessages(streamMsgs)
	return nil
}

func (s *StreamInfo) getSymmetricKeyUnsafe(streamID string) ([]byte, error) {
	key, ok := s.mapKeysListening[streamID]
	if !ok {
		return []byte{}, xerrors.Errorf("Stream does not exist.")
	}
	return key, nil
}

func (s *StreamInfo) getSymmetricKey(streamID string) ([]byte, error) {
	// Acquire lock
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()
	return s.getSymmetricKeyUnsafe(streamID)
}

func (s *StreamInfo) getClients(streamID string) ([]string, error) {
	// Acquire lock
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()
	clients, ok := s.mapClients[streamID]
	if !ok {
		return []string{}, xerrors.Errorf("Stream does not exist: %s", streamID)
	}
	return clients, nil
}

func (s *StreamInfo) getNextChunks(streamID string, numberOfChunks int) ([]types.StreamMessage, error) {
	// Acquire lock
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()
	streamMsgs, ok := s.mapListening[streamID]
	if !ok {
		return []types.StreamMessage{}, xerrors.Errorf("Stream does not exist.")
	}
	return streamMsgs[:numberOfChunks], nil
}

func (n *node) GetNextChunks(streamID string, numberOfChunks int) ([]types.StreamMessage, error) {
	return n.streamInfo.getNextChunks(streamID, numberOfChunks)
}

func (s *StreamInfo) unregisterListening(streamID string) error {
	// Acquire lock
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()
	delete(s.mapListening, streamID)
	delete(s.mapKeysListening, streamID)
	return nil
}

func (s *StreamInfo) registerStreaming(streamID string) error {
	// Acquire lock
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()
	_, ok := s.mapClients[streamID]
	if ok {
		return xerrors.Errorf("Stream already exists")
	}
	s.mapClients[streamID] = make([]string, 0)
	return nil
}

func (s *StreamInfo) unregisterStreaming(streamID string) {
	// Acquire lock
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()
	delete(s.mapClients, streamID)
	return
}
func (s *StreamInfo) addStreamClient(streamID string, clientID string) error {
	// Acquire lock
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()

	// Add client to map
	clients, ok := s.mapClients[streamID]
	if !ok {
		return xerrors.Errorf("Stream doesn't exists")
	}
	if contains(clients, clientID) {
		return xerrors.Errorf("Client already registered")
	}
	clients = append(clients, clientID)
	s.mapClients[streamID] = clients
	return nil
}

func (s *StreamInfo) removeStreamClient(streamID string, clientID string) error {
	// Acquire lock
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()

	// Add client to map
	clients, ok := s.mapClients[streamID]
	if !ok {
		return xerrors.Errorf("Stream doesn't exists")
	}
	clients = remove[string](clients, clientID)
	s.mapClients[streamID] = clients
	return nil
}

func (s *StreamInfo) addStreamingKey(streamID string, key []byte) error {
	//Acquire lock
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()

	_, ok := s.mapKeysListening[streamID]
	if ok {
		return xerrors.Errorf("Key already exists.")
	}
	s.mapKeysListening[streamID] = key
	return nil
}

// Starts streaming of provided file. returns the ID of stream.
func (n *node) Stream(data io.Reader, name string, price uint, streamID string) (err error) {

	// Get symmetric key
	symmetricKey, errK := n.streamInfo.getSymmetricKey(streamID)
	if errK != nil {
		return errK
	}

	// Craft stream info
	streamInfo := types.StreamInfo{
		StreamID:          streamID,
		Name:              name,
		Grade:             0,
		Price:             price,
		CurrentlyWatching: 0,
	}
	n.activeThreads.Add(1)
	go n.stream(data, streamInfo, symmetricKey)
	return nil
}

func (n *node) AnnounceStreaming(name string, price uint) (stringID string, err error) {
	// 1. Streamer generates the unique streamID
	streamID := xid.New().String()

	// 2. Streamer generates symmetric key that will be used for this stream
	symmetricKey, errKeyGen := generateNewAESKey()
	if errKeyGen != nil {
		log.Error().Msgf("[%s] startStreaming: Error while generating the AES key: %s",
			n.conf.Socket.GetAddress(), errKeyGen.Error())
		return "", errKeyGen
	}

	errR := n.streamInfo.registerStreaming(streamID)
	if errR != nil {
		return "", errR
	}
	errLK := n.streamInfo.addStreamingKey(streamID, symmetricKey)
	if errLK != nil {
		return "", errLK
	}

	// 3. Broadcast the start of the stream
	streamInfo := types.StreamInfo{
		StreamID:          streamID,
		Name:              name,
		Grade:             0,
		Price:             price,
		CurrentlyWatching: 0,
	}

	errB := n.broadcastStartStreaming(streamInfo)
	if errB != nil {
		n.streamInfo.unregisterStreaming(streamID)
		return "", errB
	}
	return streamID, nil
}

func (n *node) broadcastStartStreaming(streamInfo types.StreamInfo) error {
	streamStartMessage := types.StreamStartMessage{
		StreamID:   streamInfo.StreamID,
		StreamerID: n.conf.Socket.GetAddress(),
		StreamInfo: streamInfo,
	}

	transportMsg, errMarshall := n.conf.MessageRegistry.MarshalMessage(streamStartMessage)
	if errMarshall != nil {
		log.Error().Msgf("[%s] announceStreaming: Error while marshalling StreamStartMessage: %s",
			n.conf.Socket.GetAddress(), errMarshall.Error(),
		)
		return errMarshall
	}
	errBroadcast := n.Broadcast(transportMsg)
	if errBroadcast != nil {
		log.Error().Msgf("[%s] announceStreaming: Error while Broadcasting: %s",
			n.conf.Socket.GetAddress(), errBroadcast.Error(),
		)
		return errBroadcast
	}
	log.Info().Msgf("[%s] Announced streaming for %s", n.conf.Socket.GetAddress(), streamInfo.StreamID)
	return nil
}

func (n *node) stream(data io.Reader, streamInfo types.StreamInfo, symmetricKey []byte) {
	defer n.activeThreads.Done()

	// TODO : Add end of stream channel and node stopping!
	// TODO : Add sleep for streaming!

	// Read chunk by chunk
	var readBytes int

	log.Info().Msgf("[%S] STREAM STARTING %s", n.conf.Socket.GetAddress(), streamInfo.StreamID)

	for {
		time.Sleep(STREAMSLEEPTIME)

		chunk := make([]byte, n.conf.ChunkSize)
		numBytes, errRead := data.Read(chunk)
		if errRead != nil && errRead != io.EOF {
			log.Error().Msgf("[%s]: stream: error while reading the file: %s",
				n.conf.Socket.GetAddress(),
				errRead.Error())
			return
		}
		if numBytes > 0 {

			clients, err := n.streamInfo.getClients(streamInfo.StreamID)
			if err != nil {
				return
			}
			streamInfo.CurrentlyWatching = uint(len(clients))
			streamInfo.Grade, err = n.streamInfo.getGrade(streamInfo.StreamID)
			if err != nil {
				return
			}
			streamData := types.StreamData{
				StartIndex: uint(readBytes),
				EndIndex:   uint(readBytes + numBytes),
				Chunk:      chunk[:numBytes],
			}

			streamMsg := types.StreamMessage{
				StreamInfo: streamInfo,
				Data:       streamData,
			}

			payload, errConv := convertStreamMsgToPayload(streamMsg, symmetricKey)
			if errConv != nil {
				log.Error().Msgf("[%s]: stream: convertStreamMsgToPayload error: %s ",
					n.conf.Socket.GetAddress(), errConv.Error())
				readBytes = numBytes + readBytes
				continue
			}

			streamDataMsg := types.StreamDataMessage{
				ID:      streamMsg.StreamInfo.StreamID,
				Payload: payload,
			}

			transportMsg, errCast := n.conf.MessageRegistry.MarshalMessage(streamDataMsg)
			if errCast != nil {
				log.Error().Msgf("[%s]: stream: Marshalling error: %s ",
					n.conf.Socket.GetAddress(), errConv.Error())
				readBytes = numBytes + readBytes
				continue

			}
			multicastMsg := types.MulticastMessage{
				ID:      streamMsg.StreamInfo.StreamID,
				Message: &transportMsg,
			}
			errMulticast := n.Multicast(multicastMsg)
			if errMulticast != nil {
				log.Error().Msgf("[%s]: stream: Multicast for chunk [%d, %d] of stream %s failed: %s",
					n.conf.Socket.GetAddress(), readBytes,
					readBytes+numBytes, streamMsg.StreamInfo.StreamID, errMulticast.Error())
				readBytes = numBytes + readBytes
				continue
			}

		}
		readBytes = numBytes + readBytes

		// Didn't finish reading
		if errRead != io.EOF {
			continue
		}

		log.Info().Msgf("[%s] Streaming of stream %s done.", n.conf.Socket.GetAddress(), streamInfo.StreamID)
		return
	}

}

func (n *node) ConnectToStream(streamID string, streamerID string) error {

	// Craft stream join msg
	joinMsg := types.StreamConnectMessage{
		StreamID:   streamID,
		StreamerID: streamerID,
		ClientID:   n.conf.Socket.GetAddress(),
	}

	// Marshall
	transportMsg, errMarshall := n.conf.MessageRegistry.MarshalMessage(joinMsg)
	if errMarshall != nil {
		log.Error().Msgf("[%s] ConnectToStream: error while Marshalling %s",
			n.conf.Socket.GetAddress(),
			errMarshall.Error())
	}

	return n.JoinMulticast(streamID, streamerID, &transportMsg)
}

// Stop the stream
//StopStreaming(streamID string) error

// Returns the number of connected streamers for chosen stream
func (n *node) GetClients(streamID string) ([]string, error) {
	return n.streamInfo.getClients(streamID)
}

// ///////////////////////// CALLBACKS ////////////////////////////////////
// /////// CALLBACKS ////////////////////////////////////////
func (n *node) streamDataMessageCallback(msg types.Message, pkt transport.Packet) error {
	streamDataMsg, ok := msg.(*types.StreamDataMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to StreamDataMessage message got wrong type: %T", msg)
	}

	return n.handleStreamDataMsg(*streamDataMsg)
}

func (n *node) handleStreamDataMsg(message types.StreamDataMessage) error {
	// Acquire lock
	n.streamInfo.streamInfoMutex.Lock()
	defer n.streamInfo.streamInfoMutex.Unlock()

	key, err := n.streamInfo.getSymmetricKeyUnsafe(message.ID)
	if err != nil {
		return err
	}

	streamMessage, err := convertPayloadToStreamMsg(message.Payload, key)
	if err != nil {
		return err
	}

	errStr := n.streamInfo.addListeningStreamMessageUnsafe(streamMessage.StreamInfo.StreamID, streamMessage)
	if errStr != nil {
		return errStr
	}
	return nil
}

func (n *node) streamConnectMessageCallback(msg types.Message, pkt transport.Packet) error {
	streamJoinMsg, ok := msg.(*types.StreamConnectMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to StreamJoinMessage message got wrong type: %T", msg)
	}

	key, err := n.streamInfo.getSymmetricKey(streamJoinMsg.StreamID)
	if err != nil {
		return err
	}

	errAdd := n.streamInfo.addStreamClient(streamJoinMsg.StreamID, streamJoinMsg.ClientID)
	if errAdd != nil {
		return errAdd
	}

	encKey, errEnc := encryptSymmetricKey(key, streamJoinMsg.ClientID)
	if errEnc != nil {
		return errEnc
	}

	streamAcceptMsg := types.StreamAcceptMessage{
		StreamID:        streamJoinMsg.StreamID,
		StreamerID:      streamJoinMsg.StreamerID,
		ClientID:        streamJoinMsg.ClientID,
		EncSymmetricKey: encKey,
		Accepted:        true,
	}

	log.Info().Msgf("[%s] streamConnectMessageCallback: "+
		"Received stream join request for stream %s from client %s",
		n.conf.Socket.GetAddress(), streamJoinMsg.StreamID, streamJoinMsg.ClientID)

	transportMsg, errMarshall := n.conf.MessageRegistry.MarshalMessage(streamAcceptMsg)
	if errMarshall != nil {
		return errMarshall
	}
	errUnicast := n.Unicast(streamJoinMsg.ClientID, transportMsg)
	if errUnicast != nil {
		log.Info().Msgf("[%s] streamConnectMessageCallback:"+
			" Error: failed to send unicast for stream %s to client %s",
			n.conf.Socket.GetAddress(), streamJoinMsg.StreamID, streamJoinMsg.ClientID)
		return errUnicast
	}
	return nil
}

func (n *node) streamAcceptMessageCallback(msg types.Message, pkt transport.Packet) error {
	streamAcceptMsg, ok := msg.(*types.StreamAcceptMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to StreamAcceptedMessage message got wrong type: %T", msg)
	}

	if streamAcceptMsg.Accepted == false {
		return nil
	}
	key, errDecr := decryptSymmetricKey(streamAcceptMsg.EncSymmetricKey, streamAcceptMsg.ClientID)
	if errDecr != nil {
		return errDecr
	}
	err := n.streamInfo.registerListening(streamAcceptMsg.StreamID, key)
	if err != nil {
		return err
	}
	return nil
}

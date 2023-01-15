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

//const MULTICASTCHUNKSIZE = 1024 * 1024 * 12
//const STREAMSLEEPTIME = time.Millisecond

type StreamInfo struct {
	// Attributes
	mapClients   map[string][]string
	mapListening map[string][]types.StreamMessage

	lastArrivedSeq map[string]uint
	lastArrived    map[string][]types.StreamMessage

	// Rating
	rateMap map[string][]types.StreamRatingMessage

	mapFFMPG4channels map[string]chan types.StreamMessage
	availableStreams  map[string]types.StreamInfo

	// Encryption
	mapKeysListening map[string][]byte

	// Semaphores
	streamInfoMutex sync.Mutex
}

func (s *StreamInfo) processReaction(reactMsg types.StreamRatingMessage) {
	// Acquire lock
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()
	reactions, ok := s.rateMap[reactMsg.StreamID]
	if !ok {
		reactions = []types.StreamRatingMessage{}
	}
	reactions = append(reactions, reactMsg)
	s.rateMap[reactMsg.StreamID] = reactions
	_ = s.getGradeForStreamUnsafe(reactMsg.StreamID)
}

func (s *StreamInfo) getGradeForStreamUnsafe(streamID string) float64 {
	reactions, ok := s.rateMap[streamID]
	if !ok {
		return 0.0
	}
	sum := 0.0
	for _, reaction := range reactions {
		sum = sum + reaction.Grade
	}
	grade := sum / float64(len(reactions))

	// Update grade in list of all streams
	//log.Info().Msgf("Added reaction for stream %s: %ls", streamID, grade)
	streamInfo, ok := s.availableStreams[streamID]
	if !ok {
		return 0.0
	}
	streamInfo.Grade = grade
	s.availableStreams[streamID] = streamInfo
	return grade
}

func (s *StreamInfo) getGradeForStream(streamID string) float64 {
	// Acquire lock
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()
	reactions, ok := s.rateMap[streamID]
	if !ok {
		return 0.0
	}
	sum := 0.0
	for _, reaction := range reactions {
		sum = sum + reaction.Grade
	}
	grade := sum / float64(len(reactions))

	// Update grade in list of all streams
	log.Info().Msgf("Added reaction for stream %s: %v", streamID, grade)
	streamInfo, ok := s.availableStreams[streamID]
	if !ok {
		return 0.0
	}
	streamInfo.Grade = grade
	s.availableStreams[streamID] = streamInfo
	return grade
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

func (s *StreamInfo) registerFFMPG4Channel(streamID string, channel chan types.StreamMessage) error {
	// Acquire lock
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()
	_, ok := s.mapFFMPG4channels[streamID]
	if ok {
		return xerrors.Errorf("Stream already exists")
	}
	s.mapFFMPG4channels[streamID] = channel
	return nil
}

func (s *StreamInfo) notifyFFMPG4ChannelUnsafe(streamID string) {
	// Acquire lock
	channel, ok := s.mapFFMPG4channels[streamID]
	if ok {
		for _, msg := range s.mapListening[streamID] {
			channel <- msg
		}
		s.mapListening[streamID] = make([]types.StreamMessage, 0)
	}
}

func (s *StreamInfo) closeFFMPG4ChannelUnsafe(streamID string) {
	// Acquire lock
	channel, ok := s.mapFFMPG4channels[streamID]
	if ok {
		delete(s.mapFFMPG4channels, streamID)
		close(channel)
	}
}

func (s *StreamInfo) addListeningStreamMessageUnsafe(streamID string, msg types.StreamMessage) error {

	//log.Info().Msgf("Adding seq num %d", msg.Data.SeqNum)
	lastArrivedSeq, ok := s.lastArrivedSeq[streamID]
	if !ok {
		s.lastArrivedSeq[streamID] = 0
		lastArrivedSeq = 0
	}
	arrived, exist := s.lastArrived[streamID]
	if !exist {
		arrived = []types.StreamMessage{}
	}
	if lastArrivedSeq == msg.Data.SeqNum {
		s.lastArrived[streamID] = append(arrived, msg)
		return nil
	}
	s.lastArrivedSeq[streamID] = msg.Data.SeqNum
	s.lastArrived[streamID] = []types.StreamMessage{msg}

	if len(arrived) == 0 {
		return nil
	}
	log.Info().Msgf("Received seq num %d", lastArrivedSeq)

	reconstruct := sortStreamMessages(arrived)
	var chunk []byte
	for _, rm := range reconstruct {
		chunk = append(chunk, rm.Data.Chunk...)
	}

	reconstructedMsg := types.StreamMessage{
		StreamInfo: msg.StreamInfo,
		Data: types.StreamData{
			StartIndex: lastArrivedSeq,
			Chunk:      chunk,
			EndIndex:   lastArrivedSeq + 1,
		},
	}

	streamMsgs, ok := s.mapListening[streamID]
	if !ok {
		return xerrors.Errorf("Stream does not exist.")
	}
	streamMsgs = append(streamMsgs, reconstructedMsg)
	s.mapListening[streamID] = sortStreamMessages(streamMsgs)
	log.Info().Msgf("Notify receiver")
	s.notifyFFMPG4ChannelUnsafe(streamID)
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
	lastArrivedstreamMsgs, exist := s.lastArrived[streamID]
	streamMsgs = append(streamMsgs, lastArrivedstreamMsgs...)

	if !ok && !exist {
		return []types.StreamMessage{}, xerrors.Errorf("Stream does not exist.")
	}
	if numberOfChunks == -1 {
		s.mapListening[streamID] = []types.StreamMessage{}
		s.lastArrived[streamID] = []types.StreamMessage{}
		return streamMsgs, nil
	}
	retVal := streamMsgs[:numberOfChunks]
	s.mapListening[streamID] = streamMsgs[numberOfChunks:]
	s.lastArrived[streamID] = []types.StreamMessage{}
	retVal = sortStreamMessages(retVal)
	return retVal, nil
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
	s.closeFFMPG4ChannelUnsafe(streamID)
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

	// Update list of all available streams
	streamInfo, ok := s.availableStreams[streamID]
	if !ok {
		return nil
	}
	streamInfo.CurrentlyWatching = uint(len(clients))
	s.availableStreams[streamID] = streamInfo
	return nil
}

func (s *StreamInfo) RemoveStreamClient(streamID string, clientID string) error {
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

// Stream starts streaming of provided file. returns the ID of stream.
func (n *node) Stream(
	data io.Reader,
	name string,
	price uint,
	streamID string,
	thumbnail []byte,
	sequenceNum uint,
) (err error) {

	// Get symmetric key
	symmetricKey, errK := n.streamInfo.getSymmetricKey(streamID)
	if errK != nil {
		return errK
	}

	// Craft stream info
	streamInfo := types.StreamInfo{
		StreamID:          streamID,
		StreamerID:        n.conf.Socket.GetAddress(),
		Name:              name,
		Grade:             0,
		Price:             price,
		CurrentlyWatching: 0,
		Thumbnail:         thumbnail,
	}
	n.stream(data, streamInfo, symmetricKey, sequenceNum)
	return nil
}

func (n *node) AnnounceStartStreaming(name string, price uint, thumbnail []byte) (stringID string, err error) {
	// 1. Streamer generates the unique streamID
	streamID := xid.New().String()

	// 2. Streamer generates symmetric key that will be used for this stream
	symmetricKey, errKeyGen := generateNewAESKey()
	if errKeyGen != nil {
		log.Error().Msgf(
			"[%s] startStreaming: Error while generating the AES key: %s",
			n.conf.Socket.GetAddress(), errKeyGen.Error(),
		)
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
		Thumbnail:         thumbnail,
		StreamerID:        n.conf.Socket.GetAddress(),
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
		log.Error().Msgf(
			"[%s] announceStreaming: Error while marshalling StreamStartMessage: %s",
			n.conf.Socket.GetAddress(), errMarshall.Error(),
		)
		return errMarshall
	}
	errBroadcast := n.Broadcast(transportMsg)
	if errBroadcast != nil {
		log.Error().Msgf(
			"[%s] announceStreaming: Error while Broadcasting: %s",
			n.conf.Socket.GetAddress(), errBroadcast.Error(),
		)
		return errBroadcast
	}
	log.Info().Msgf("[%s] Announced streaming for %s", n.conf.Socket.GetAddress(), streamInfo.StreamID)
	return nil
}

func (n *node) stream(data io.Reader, streamInfo types.StreamInfo, symmetricKey []byte, seqNum uint) {
	// Read chunk by chunk
	var readBytes int

	log.Info().Msgf("[%s] STREAM STARTING %s", n.conf.Socket.GetAddress(), streamInfo.StreamID)

	for {
		//time.Sleep(STREAMSLEEPTIME)

		chunk := make([]byte, n.conf.ChunkSize)
		numBytes, errRead := data.Read(chunk)
		if errRead != nil && errRead != io.EOF {
			log.Error().Msgf(
				"[%s]: stream: error while reading the file: %s",
				n.conf.Socket.GetAddress(),
				errRead.Error(),
			)
			return
		}
		if numBytes > 0 {

			clients, err := n.streamInfo.getClients(streamInfo.StreamID)
			if err != nil {
				return
			}
			streamInfo.CurrentlyWatching = uint(len(clients))
			streamInfo.Grade = n.streamInfo.getGradeForStream(streamInfo.StreamID)

			streamData := types.StreamData{
				StartIndex: uint(readBytes),
				EndIndex:   uint(readBytes + numBytes),
				SeqNum:     seqNum,
				Chunk:      chunk[:numBytes],
			}

			streamMsg := types.StreamMessage{
				StreamInfo: streamInfo,
				Data:       streamData,
			}

			payload, errConv := convertStreamMsgToPayload(streamMsg, symmetricKey)
			if errConv != nil {
				readBytes = numBytes + readBytes
				continue
			}

			errCast := n.multicastStreamData(streamMsg, payload)
			if errCast != nil {
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
func (n *node) multicastStreamData(streamMsg types.StreamMessage, payload string) error {
	streamDataMsg := types.StreamDataMessage{
		ID:      streamMsg.StreamInfo.StreamID,
		Payload: payload,
	}

	transportMsg, errCast := n.conf.MessageRegistry.MarshalMessage(streamDataMsg)
	if errCast != nil {
		log.Error().Msgf(
			"[%s]: stream: Marshalling error: %s ",
			n.conf.Socket.GetAddress(), errCast.Error(),
		)
		return errCast

	}
	multicastMsg := types.MulticastMessage{
		ID:      streamMsg.StreamInfo.StreamID,
		Message: &transportMsg,
	}
	errMulticast := n.Multicast(multicastMsg)
	return errMulticast
}
func (n *node) getStreamInfo(streamID string) (types.StreamInfo, error) {
	n.streamInfo.streamInfoMutex.Lock()
	defer n.streamInfo.streamInfoMutex.Unlock()
	streamInfo, ok := n.streamInfo.availableStreams[streamID]
	if !ok {
		return types.StreamInfo{}, xerrors.Errorf("Stream %s does not exist", streamID)
	}
	return streamInfo, nil
}
func (n *node) ConnectToStream(streamID string, streamerID string) error {

	// Check if node has enough money
	streamInfo, errS := n.getStreamInfo(streamID)
	if errS != nil {
		log.Error().Msgf("[%s]ConnectToStream: error:%s", n.conf.Socket.GetAddress(), errS.Error())
		return errS
	}
	amount, errAmount := n.GetAmount(n.conf.Socket.GetAddress())
	if errAmount != nil {
		log.Error().Msgf("[%s]ConnectToStream: getAmountError:%s", n.conf.Socket.GetAddress(), errAmount.Error())
		return errAmount
	}

	if amount < float64(streamInfo.Price) {
		return xerrors.Errorf("Not enough money!")
	}

	errPaySubscription := n.PaySubscription(n.conf.Socket.GetAddress(), streamerID, streamID, float64(streamInfo.Price))
	if errPaySubscription != nil {
		log.Error().Msgf(
			"[%s]ConnectToStream: PaySubscriptionError:%s",
			n.conf.Socket.GetAddress(), errPaySubscription.Error(),
		)
		return errPaySubscription
	}

	// Wait for the payment to be processed
	time.Sleep(500 * time.Millisecond)

	// Craft stream join msg
	joinMsg := types.StreamConnectMessage{
		StreamID:   streamID,
		StreamerID: streamerID,
		ClientID:   n.conf.Socket.GetAddress(),
	}

	// Marshall
	transportMsg, errMarshall := n.conf.MessageRegistry.MarshalMessage(joinMsg)
	if errMarshall != nil {
		log.Error().Msgf(
			"[%s] ConnectToStream: error while Marshalling %s",
			n.conf.Socket.GetAddress(),
			errMarshall.Error(),
		)
	}

	return n.JoinMulticast(streamID, streamerID, &transportMsg)
}

// GetClients returns the number of connected streamers for chosen stream
func (n *node) GetClients(streamID string) ([]string, error) {
	return n.streamInfo.getClients(streamID)
}

// AnnounceStopStreaming Stops the stream
func (n *node) AnnounceStopStreaming(streamID string) error {

	_, errC := n.streamInfo.getClients(streamID)
	if errC != nil {
		return errC
	}
	n.streamInfo.unregisterStreaming(streamID)
	// Craft StopStreamingMessage
	stopStreamMsg := types.StreamStopMessage{
		StreamID:   streamID,
		StreamerID: n.conf.Socket.GetAddress(),
	}

	// Marshall msg
	transportMsg, errMarshall := n.conf.MessageRegistry.MarshalMessage(stopStreamMsg)
	if errMarshall != nil {
		return errMarshall
	}
	err := n.StopMulticast(streamID, transportMsg)
	if err != nil {
		return err
	}
	return nil
}

func (s *StreamInfo) registerAvailableStream(stream types.StreamInfo) {
	log.Info().Msgf("stream available " + stream.Name)
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()
	s.availableStreams[stream.StreamID] = stream
}

func (s *StreamInfo) updateAvailableStreamUnsafe(stream types.StreamInfo) {
	streamInfo, ok := s.availableStreams[stream.StreamID]
	if ok {
		// preserve the pic
		stream.Thumbnail = streamInfo.Thumbnail
	}
	s.availableStreams[stream.StreamID] = stream

}

func (s *StreamInfo) unregisterAvailableStream(streamID string) {
	s.streamInfoMutex.Lock()
	defer s.streamInfoMutex.Unlock()
	delete(s.availableStreams, streamID)
}

func (n *node) ReactToStream(streamID string, streamerID string, grade float64) error {
	if n.conf.AnonymousReact {
		return n.CrowdsReact(streamID, streamerID, grade)
	}

	// Check if I am listening to a stream
	_, err := n.streamInfo.getSymmetricKey(streamID)
	if err != nil {
		log.Error().Msgf("[%s] React to stream error: %s", n.conf.Socket.GetAddress(), err.Error())
		return err
	}

	reactMsg := types.StreamRatingMessage{
		StreamID:   streamID,
		StreamerID: streamerID,
		Grade:      grade,
	}

	// Marshall msg
	transportMsg, errMarshall := n.conf.MessageRegistry.MarshalMessage(reactMsg)
	if errMarshall != nil {
		log.Error().Msgf(
			"[%s]:ReactToStream: Error marshalling: %s",
			n.conf.Socket.GetAddress(), errMarshall.Error(),
		)
		return errMarshall
	}

	errUnicast := n.Unicast(streamerID, transportMsg)
	if errUnicast != nil {
		log.Error().Msgf(
			"[%s]:ReactToStream: Error unicast: %s",
			n.conf.Socket.GetAddress(), errUnicast.Error(),
		)
		return errUnicast
	}
	return nil
}

func (n *node) GetAllStreams() []types.StreamInfo {
	n.streamInfo.streamInfoMutex.Lock()
	defer n.streamInfo.streamInfoMutex.Unlock()
	tmp := make([]types.StreamInfo, 0)
	for _, val := range n.streamInfo.availableStreams {
		tmp = append(tmp, val)
	}
	return tmp
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

	n.streamInfo.updateAvailableStreamUnsafe(streamMessage.StreamInfo)
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

	// Check if client has pay
	streamInfo, errInfo := n.getStreamInfo(streamJoinMsg.StreamID)
	if errInfo != nil {
		return errInfo
	}
	isPayed, errPayed := n.IsPayedSubscription(
		streamJoinMsg.ClientID, streamJoinMsg.StreamerID,
		streamJoinMsg.StreamID, float64(streamInfo.Price),
	)
	if errPayed != nil {
		return errPayed
	}

	if !isPayed {
		log.Info().Msgf(
			"[%s] Not payed subscription: Client: %s, Streamer: %s, Stream %s",
			n.conf.Socket.GetAddress(), streamJoinMsg.ClientID, streamJoinMsg.StreamerID, streamJoinMsg.StreamID,
		)
		return nil
	}

	key, err := n.streamInfo.getSymmetricKey(streamJoinMsg.StreamID)
	if err != nil {
		return err
	}

	errAdd := n.streamInfo.addStreamClient(streamJoinMsg.StreamID, streamJoinMsg.ClientID)
	if errAdd != nil {
		return errAdd
	}

	encKey, errEnc := n.encryptSymmetricKey(key, streamJoinMsg.ClientID)
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

	log.Info().Msgf(
		"[%s] streamConnectMessageCallback: "+
			"Received stream join request for stream %s from client %s",
		n.conf.Socket.GetAddress(), streamJoinMsg.StreamID, streamJoinMsg.ClientID,
	)

	transportMsg, errMarshall := n.conf.MessageRegistry.MarshalMessage(streamAcceptMsg)
	if errMarshall != nil {
		return errMarshall
	}
	errUnicast := n.Unicast(streamJoinMsg.ClientID, transportMsg)
	if errUnicast != nil {
		log.Info().Msgf(
			"[%s] streamConnectMessageCallback:"+
				" Error: failed to send unicast for stream %s to client %s",
			n.conf.Socket.GetAddress(), streamJoinMsg.StreamID, streamJoinMsg.ClientID,
		)
		return errUnicast
	}
	return nil
}

func (n *node) streamAcceptMessageCallback(msg types.Message, pkt transport.Packet) error {
	streamAcceptMsg, ok := msg.(*types.StreamAcceptMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to StreamAcceptedMessage message got wrong type: %T", msg)
	}

	log.Info().Msgf(
		"[%s] stream accept %s accepted %v", n.conf.Socket.GetAddress(),
		streamAcceptMsg.StreamID, streamAcceptMsg.Accepted,
	)

	if !streamAcceptMsg.Accepted {
		return nil
	}
	key, errDecr := n.decryptSymmetricKey(streamAcceptMsg.EncSymmetricKey, streamAcceptMsg.ClientID)
	if errDecr != nil {
		return errDecr
	}
	err := n.streamInfo.registerListening(streamAcceptMsg.StreamID, key)
	if err != nil {
		return err
	}
	return nil
}

func (n *node) streamStopMessageCallback(msg types.Message, pkt transport.Packet) error {
	streamStopMsg, ok := msg.(*types.StreamStopMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to StreamStopMessage message got wrong type: %T", msg)
	}

	log.Info().Msgf(
		"[%s] STREAM FINISHED  number of packets",
		n.conf.Socket.GetAddress(),
	)
	n.streamInfo.unregisterAvailableStream(streamStopMsg.StreamID)
	err := n.streamInfo.unregisterListening(streamStopMsg.StreamID)
	if err != nil {
		return err
	}
	return nil
}

func (n *node) streamStartMessageCallback(msg types.Message, pkt transport.Packet) error {
	streamStartMsg, ok := msg.(*types.StreamStartMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to streamStartMessage message got wrong type: %T", msg)
	}
	n.streamInfo.registerAvailableStream(streamStartMsg.StreamInfo)
	return nil
}

func (n *node) streamRatingMessageCallback(msg types.Message, pkt transport.Packet) error {
	streamRatingMsg, ok := msg.(*types.StreamRatingMessage)
	if !ok {
		return xerrors.Errorf("Failed to cast to streamRatingMessage message got wrong type: %T", msg)
	}
	n.streamInfo.processReaction(*streamRatingMsg)
	return nil
}

// StreamingInit initializes streaming
func (n *node) StreamingInit() {
	n.conf.MessageRegistry.RegisterMessageCallback(types.StreamDataMessage{}, n.streamDataMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StreamAcceptMessage{}, n.streamAcceptMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StreamConnectMessage{}, n.streamConnectMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StreamStartMessage{}, n.streamStartMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StreamStopMessage{}, n.streamStopMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StreamRatingMessage{}, n.streamRatingMessageCallback)
	n.streamInfo.lastArrivedSeq = make(map[string]uint)
	n.streamInfo.lastArrived = make(map[string][]types.StreamMessage)
	n.streamInfo.availableStreams = make(map[string]types.StreamInfo)
	n.streamInfo.rateMap = make(map[string][]types.StreamRatingMessage)
}

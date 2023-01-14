package impl

import (
	"math/rand"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

type CrowdsInfo struct {
	chunkMap        *AtomicChunkMap
	chunkChannelMap *AtomicChannelTable
	noEncryption    bool
}

func (n *node) CrowdsSend(peers []string, body, to string) error {
	if !contains(peers, n.conf.Socket.GetAddress()) {
		peers = append(peers, n.conf.Socket.GetAddress())
	}

	crowdsMessagingReqMsgMarshalled, err := n.CreateCrowdsMessagingRequest(to, body)
	if err != nil {
		return err
	}

	err = n.SendCrowdsMessage(&crowdsMessagingReqMsgMarshalled, peers)
	if err != nil {
		return err
	}
	n.chatInfo.AddMessage(body, to)

	return nil
}

func (n *node) CrowdsDownload(peers []string, filename string) error {
	if !contains(peers, n.conf.Socket.GetAddress()) {
		peers = append(peers, n.conf.Socket.GetAddress())
	}

	requestID := xid.New().String()
	reqChannel := make(chan string)
	n.crowdsInfo.chunkChannelMap.StoreChannel(requestID, reqChannel)

	metahash := n.Resolve(filename)
	if metahash == "" { // empty metahash
		metahash = filename
		log.Info().Msgf("crowds initator = %s; metahash is empty, filename will be used as metahash",
			n.conf.Socket.GetAddress())
	}

	crowdsDownloadReqMsgMarshalled, err := n.CreateCrowdsDownloadRequest(requestID, metahash)
	if err != nil {
		return err
	}

	err = n.SendCrowdsMessage(&crowdsDownloadReqMsgMarshalled, peers)
	if err != nil {
		return err
	}

	for {
		select {
		case <-n.notifyEnd:
			return nil

		case <-reqChannel:
			log.Info().Msgf("node %s skinuto film", n.conf.Socket.GetAddress())
			file := n.crowdsInfo.chunkMap.GetFile(requestID)
			if file == nil {
				return nil
			}

			err = os.WriteFile("./downloaded_"+filename, file, 0644)
			if err != nil {
				log.Error().Msgf("error while writing file to disc %s", err)
			}

			return nil
		}
	}
}

func (n *node) SendCrowdsMessage(embeddedMsg *transport.Message, recipients []string) error {
	crowdsMsg := types.CrowdsMessage{
		Msg:        embeddedMsg,
		Recipients: recipients,
	}

	crowdsMsgMarshalled, err := n.conf.MessageRegistry.MarshalMessage(crowdsMsg)
	if err != nil {
		return err
	}

	to := n.conf.Socket.GetAddress()
	for to == n.conf.Socket.GetAddress() {
		peerIdx := rand.Intn(len(crowdsMsg.Recipients))
		to = crowdsMsg.Recipients[peerIdx]
	}

	if n.crowdsInfo.noEncryption {
		return n.Unicast(to, crowdsMsgMarshalled)
	}

	publicKey, err := n.GetPublicKey(to)
	if err != nil {
		return err
	}

	confidMsg, err := n.CreateConfidentialityMsg(crowdsMsgMarshalled, publicKey)
	if err != nil {
		return err
	}

	confidMsgMarshalled, err := n.conf.MessageRegistry.MarshalMessage(confidMsg)
	if err != nil {
		return err
	}

	return n.Unicast(to, confidMsgMarshalled)
}

func (n *node) CreateCrowdsMessagingRequest(dst, content string) (transport.Message, error) {
	chatMsg := types.ChatMessage{Message: content}
	chatMsgMarshalled, err := n.conf.MessageRegistry.MarshalMessage(chatMsg)
	if err != nil {
		return transport.Message{}, err
	}

	crowdsMessagingReqMsg := types.CrowdsMessagingRequestMessage{
		FinalDst: dst,
		Msg:      &chatMsgMarshalled,
	}

	if !n.crowdsInfo.noEncryption {
		return n.conf.MessageRegistry.MarshalMessage(crowdsMessagingReqMsg)
	}

	// Confidentiality.
	publicKey, err := n.GetPublicKey(dst)
	if err != nil {
		return transport.Message{}, err
	}

	confidMsg, err := n.CreateConfidentialityMsg(chatMsgMarshalled, publicKey)
	if err != nil {
		return transport.Message{}, err
	}

	confidMsgMarshalled, err := n.conf.MessageRegistry.MarshalMessage(confidMsg)
	if err != nil {
		return transport.Message{}, err
	}

	crowdsMessagingReqMsg.Msg = &confidMsgMarshalled

	return n.conf.MessageRegistry.MarshalMessage(crowdsMessagingReqMsg)
}

func (n *node) CreateCrowdsDownloadRequest(reqID, content string) (transport.Message, error) {
	crowdsDownloadReqMsg := types.CrowdsDownloadRequestMessage{
		Origin:    n.conf.Socket.GetAddress(),
		RequestID: reqID,
		Key:       content,
	}

	return n.conf.MessageRegistry.MarshalMessage(crowdsDownloadReqMsg)
}

func (n *node) CrowdsInit(conf peer.Configuration) {
	log.Info().Msgf("Registering crowds-callbacks for node %s", conf.Socket.GetAddress())
	conf.MessageRegistry.RegisterMessageCallback(types.CrowdsMessage{}, n.CrowdsMessageCallback)
	conf.MessageRegistry.RegisterMessageCallback(
		types.CrowdsMessagingRequestMessage{},
		n.CrowdsMessagingRequestMessageCallback,
	)
	conf.MessageRegistry.RegisterMessageCallback(
		types.CrowdsDownloadRequestMessage{},
		n.CrowdsDownloadRequestMessageCallback,
	)
	conf.MessageRegistry.RegisterMessageCallback(
		types.CrowdsDownloadReplyMessage{},
		n.CrowdsDownloadReplyMessageCallback,
	)
}

func (n *node) CrowdsDestroy() {
	n.crowdsInfo.chunkChannelMap.CloseDeleteAll()
}

/************
* CALLBACKS *
*************/

func (n *node) CrowdsMessageCallback(msg types.Message, pkt transport.Packet) error {
	// Cast the message to its actual type. You assume it is the right type.
	crowdsMsg, ok := msg.(*types.CrowdsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// If rand <= then deliver message
	if rand.Float64() <= n.conf.CrowdsProbability {
		return n.conf.MessageRegistry.ProcessPacket(transport.Packet{Header: pkt.Header, Msg: crowdsMsg.Msg})
	}

	// If rand > keep crowds msging
	return n.SendCrowdsMessage(crowdsMsg.Msg, crowdsMsg.Recipients)
}

func (n *node) CrowdsMessagingRequestMessageCallback(msg types.Message, _ transport.Packet) error {
	// Cast the message to its actual type. You assume it is the right type.
	crowdsMessagingReqMsg, ok := msg.(*types.CrowdsMessagingRequestMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	return n.Unicast(crowdsMessagingReqMsg.FinalDst, *crowdsMessagingReqMsg.Msg)
}

func (n *node) CrowdsDownloadRequestMessageCallback(msg types.Message, _ transport.Packet) error {
	// Cast the message to its actual type. You assume it is the right type.
	crowdsDownloadReqMsg, ok := msg.(*types.CrowdsDownloadRequestMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	err := n.DownloadAndTransmit(crowdsDownloadReqMsg.Key, crowdsDownloadReqMsg)
	if err != nil {
		return n.TransmitChunk(nil, 0, 0, "", crowdsDownloadReqMsg)
	}

	return n.DownloadAndTransmit(crowdsDownloadReqMsg.Key, crowdsDownloadReqMsg)
}

func (n *node) CrowdsDownloadReplyMessageCallback(msg types.Message, _ transport.Packet) error {
	// Cast the message to its actual type. You assume it is the right type.
	crowdsDownloadReplyMsg, ok := msg.(*types.CrowdsDownloadReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// If an error happened close everything.
	if crowdsDownloadReplyMsg.Value == nil && crowdsDownloadReplyMsg.Metahash == "" &&
		crowdsDownloadReplyMsg.Index == 0 && crowdsDownloadReplyMsg.TotalChunks == 0 {
		n.crowdsInfo.chunkChannelMap.CloseDelete(crowdsDownloadReplyMsg.RequestID)
		return nil
	}

	// Store locally.
	n.conf.Storage.GetDataBlobStore().Set(crowdsDownloadReplyMsg.Metahash, crowdsDownloadReplyMsg.Value)

	// If it was metafile, just store in blob and exit.
	if crowdsDownloadReplyMsg.Index == 0 && crowdsDownloadReplyMsg.TotalChunks == 0 {
		return nil
	}

	fileDownloaded := n.crowdsInfo.chunkMap.Add(crowdsDownloadReplyMsg)
	if fileDownloaded {
		n.crowdsInfo.chunkChannelMap.CloseDelete(crowdsDownloadReplyMsg.RequestID)
	}

	return nil
}

func (n *node) DownloadAndTransmit(metahash string, msg *types.CrowdsDownloadRequestMessage) error {

	filename := n.GetFileNameFromMetaHash(metahash)
	if filename == "" {
		return xerrors.Errorf("node %s could not find filename for given metahash %s during crowds download",
			n.conf.Socket.GetAddress(), metahash)
	}

	_, err := n.SearchAll(*regexp.MustCompile(filename), 10, time.Second*4) // update catalog.
	time.Sleep(time.Second * 4)
	if err != nil {
		log.Error().Msgf("[%s] error during search all in crowds: %s",
			n.conf.Socket.GetAddress(), err.Error())
		return err
	}
	log.Info().Msgf("node %s catalog is %s", n.conf.Socket.GetAddress(), n.GetCatalog())

	// Get metafile
	metafileValueBytes, err := n.getValueForMetahash(metahash)
	if err != nil {
		log.Error().Msgf("could not get name for metahash %s, error: %s", metahash, err.Error())
		return err
	}

	if uint(len(metafileValueBytes)) > n.conf.ChunkSize {
		return xerrors.Errorf("Metafile is larger than 1 chunk")
	}

	chunkIdx := uint(0)
	err = n.TransmitChunk(metafileValueBytes, chunkIdx, 0, metahash, msg)
	if err != nil {
		return err
	}

	// Extract parts of the file
	fileParts := strings.Split(string(metafileValueBytes), peer.MetafileSep)

	var mapOfParts = make(map[string][]byte)

	for _, key := range fileParts {
		// Get value locally or remotely

		log.Info().Msgf("node %s pokusava da skine za mh %s", n.conf.Socket.GetAddress(), key)
		chunk, errValue := n.getValueForMetahash(key)
		if errValue != nil {
			log.Info().Msgf("error tokom dohvatanja chunka %s", errValue)
			return errValue
		}

		chunkIdx++
		log.Info().Msgf("node %s ide na transmit chunks", n.conf.Socket.GetAddress())
		err = n.TransmitChunk(chunk, chunkIdx, len(fileParts), key, msg)
		if err != nil {
			return err
		}

		// Add to map
		mapOfParts[key] = chunk
	}

	// Store locally if successful
	for k, v := range mapOfParts {
		n.conf.Storage.GetDataBlobStore().Set(k, v)
	}
	return nil
}

func (n *node) TransmitChunk(
	chunk []byte,
	chunkIdx uint,
	numChunks int,
	key string,
	msg *types.CrowdsDownloadRequestMessage,
) error {
	crowdsDownloadReplyMsg := types.CrowdsDownloadReplyMessage{
		RequestID:   msg.RequestID,
		Key:         msg.Key,
		Index:       chunkIdx - 1,
		Value:       chunk,
		Metahash:    key,
		TotalChunks: uint(numChunks),
	}

	crowdsDownloadReplyMsgMarshalled, err := n.conf.MessageRegistry.MarshalMessage(crowdsDownloadReplyMsg)
	if err != nil {
		return err
	}

	if n.crowdsInfo.noEncryption {
		return n.Unicast(msg.Origin, crowdsDownloadReplyMsgMarshalled)
	}

	publicKey, err := n.GetPublicKey(msg.Origin)
	if err != nil {
		return err
	}

	confidMsg, err := n.CreateConfidentialityMsg(crowdsDownloadReplyMsgMarshalled, publicKey)
	if err != nil {
		return err
	}

	confidMsgMarshalled, err := n.conf.MessageRegistry.MarshalMessage(confidMsg)
	if err != nil {
		return err
	}

	return n.Unicast(msg.Origin, confidMsgMarshalled)
}

func (n *node) GetFileNameFromMetaHash(metahash string) string {
	filename := ""
	n.conf.Storage.GetNamingStore().ForEach(func(key string, val []byte) bool {
		if metahash != string(val) {
			return true
		}

		filename = key
		return false
	})

	return filename
}

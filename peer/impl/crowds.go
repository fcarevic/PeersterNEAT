package impl

import (
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
	"strings"
)

type CrowdsInfo struct {
	chunkMap        *AtomicChunkMap
	chunkChannelMap *AtomicChannelTable
}

/*
 */
func (n *node) StartCrowds(peers []string, isFileRequest bool, content, finalDst string) ([]byte, error) {
	if !isFileRequest {
		crowdsMessagingReqMsgMarshalled, err := n.CreateCrowdsMessagingRequest(finalDst, content)
		if err != nil {
			return nil, err
		}

		err = n.SendCrowdsMessage(&crowdsMessagingReqMsgMarshalled, peers)
		return []byte(""), nil
	}

	requestID := xid.New().String()
	reqChannel := make(chan string)
	n.crowdsInfo.chunkChannelMap.StoreChannel(requestID, reqChannel)

	crowdsDownloadReqMsgMarshalled, err := n.CreateCrowdsDownloadRequest(requestID, content)
	if err != nil {
		return nil, err
	}

	err = n.SendCrowdsMessage(&crowdsDownloadReqMsgMarshalled, peers)
	if err != nil {
		return nil, err
	}

	for {
		select {
		case <-n.notifyEnd:
			return []byte(""), nil

		case <-reqChannel:
			return n.crowdsInfo.chunkMap.GetFile(requestID), nil
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

	peer := n.conf.Socket.GetAddress()
	for peer == n.conf.Socket.GetAddress() {
		peerIdx := rand.Intn(len(crowdsMsg.Recipients))
		peer = crowdsMsg.Recipients[peerIdx]
	}

	// TODO: this msg should be embedded in ConfidMessage with publickey of peer!
	return n.Unicast(peer, crowdsMsgMarshalled)
}

func (n *node) CreateCrowdsMessagingRequest(dst, content string) (transport.Message, error) {
	// TODO: this should be ConfidMessage
	chatMsg := types.ChatMessage{
		Message: content,
	}
	chatMsgMarshalled, err := n.conf.MessageRegistry.MarshalMessage(chatMsg)
	if err != nil {
		return transport.Message{}, err
	}

	crowdsMessagingReqMsg := types.CrowdsMessagingRequestMessage{
		FinalDst: dst,
		Msg:      &chatMsgMarshalled,
	}

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
	conf.MessageRegistry.RegisterMessageCallback(types.CrowdsMessagingRequestMessage{}, n.CrowdsMessagingRequestMessageCallback)
	conf.MessageRegistry.RegisterMessageCallback(types.CrowdsDownloadRequestMessage{}, n.CrowdsDownloadRequestMessageCallback)
	conf.MessageRegistry.RegisterMessageCallback(types.CrowdsDownloadReplyMessage{}, n.CrowdsDownloadReplyMessageCallback)
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
	if rand.Float64() <= n.conf.ContinueMongering {
		return n.conf.MessageRegistry.ProcessPacket(transport.Packet{Header: pkt.Header, Msg: crowdsMsg.Msg})
	}

	// If rand > keep crowds msging
	return n.SendCrowdsMessage(crowdsMsg.Msg, crowdsMsg.Recipients)
}

func (n *node) CrowdsMessagingRequestMessageCallback(msg types.Message, pkt transport.Packet) error {
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

	return n.DownloadAndTransmit(crowdsDownloadReqMsg.Key, crowdsDownloadReqMsg)
}

func (n *node) CrowdsDownloadReplyMessageCallback(msg types.Message, _ transport.Packet) error {
	// Cast the message to its actual type. You assume it is the right type.
	crowdsDownloadReplyMsg, ok := msg.(*types.CrowdsDownloadReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	fileDownloaded := n.crowdsInfo.chunkMap.Add(crowdsDownloadReplyMsg)
	if fileDownloaded {
		n.crowdsInfo.chunkChannelMap.CloseDelete(crowdsDownloadReplyMsg.RequestID)
	}

	return nil
}

func (n *node) DownloadAndTransmit(metahash string, msg *types.CrowdsDownloadRequestMessage) error {
	// Get metafile
	metafileValueBytes, err := n.getValueForMetahash(metahash)
	if err != nil {
		return err
	}

	if uint(len(metafileValueBytes)) > n.conf.ChunkSize {
		return xerrors.Errorf("Metafile is larger than 1 chunk")
	}

	// Extract parts of the file
	fileParts := strings.Split(string(metafileValueBytes), peer.MetafileSep)

	var mapOfParts = make(map[string][]byte)
	chunkIdx := uint(0)

	for _, key := range fileParts {
		// Get value locally or remotely
		chunk, errValue := n.getValueForMetahash(key)
		if errValue != nil {
			return errValue
		}

		chunkIdx++
		err = n.TransmitChunk(chunk, chunkIdx, len(fileParts), msg)
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

func (n *node) TransmitChunk(chunk []byte, chunkIdx uint, numChunks int, msg *types.CrowdsDownloadRequestMessage) error {
	crowdsDownloadReplyMsg := types.CrowdsDownloadReplyMessage{
		RequestID:   msg.RequestID,
		Key:         msg.Key,
		Index:       chunkIdx - 1,
		Value:       chunk,
		TotalChunks: uint(numChunks),
	}

	crowdsDownloadReplyMsgMarshalled, err := n.conf.MessageRegistry.MarshalMessage(crowdsDownloadReplyMsg)
	if err != nil {
		return err
	}

	return n.Unicast(msg.Origin, crowdsDownloadReplyMsgMarshalled)
}
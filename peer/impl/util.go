package impl

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"io"
	"sort"
	"strconv"
	"strings"
)

// TTL Define const
const TTL = 0
const AESKEYSIZE = 32

// Util function to craft the Packet given the message
func msgToPacket(src string, relay string, dest string, msg transport.Message) transport.Packet {

	// Craft new header
	var header = transport.NewHeader(src, relay, dest, TTL)

	// Craft packet
	return transport.Packet{
		Header: &header,
		Msg:    &msg}
}

// Util function to craft the Packet given the message
func (n *node) msgTypesToPacket(src string, relay string, dest string, msg types.Message) (transport.Packet, error) {

	// Marshall message
	msgMarshalled, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		return transport.Packet{}, err
	}

	// Craft packet
	pkt := msgToPacket(src, relay, dest, msgMarshalled)
	return pkt, nil
}

// Check if string is in the slice
// Code taken from https://gosamples.dev/slice-contains/
func contains(elems []string, v string) bool {
	for _, s := range elems {
		if v == s {
			return true
		}
	}
	return false
}

func copyStatusRumorMaps(
	statusMap map[string]uint,
	rumorMap map[string][]types.Rumor) (map[string]uint,
	map[string][]types.Rumor) {
	var statusCopy = make(map[string]uint)
	for k, v := range statusMap {
		statusCopy[k] = v
	}

	var rumorMapCopy = make(map[string][]types.Rumor)

	for peer, rumorArray := range rumorMap {
		var rumorArrayCopy []types.Rumor
		rumorArrayCopy = append(rumorArrayCopy, rumorArray...)
		rumorMapCopy[peer] = rumorArrayCopy
	}
	return statusCopy, rumorMapCopy
}

// Sort rumors
func sortRumors(rumors []types.Rumor) []types.Rumor {
	sort.Slice(rumors, func(i, j int) bool {
		return rumors[i].Sequence < rumors[j].Sequence
	})
	return rumors
}

func createBlockchainBlock(step uint, previousHash string, value types.PaxosValue) types.BlockchainBlock {
	str := strconv.Itoa(int(step)) + value.UniqID + value.Filename + value.Metahash + previousHash
	hash := hashSHA256([]byte(str))
	block := types.BlockchainBlock{
		Index:    step,
		Value:    value,
		PrevHash: []byte(previousHash),
		Hash:     hash,
	}

	return block
}

// decryptAESGCM decrypts the ciphertext in AES GCM mode
func decryptAESGCM(key []byte, ciphertext []byte) ([]byte, error) {
	c, errC := aes.NewCipher(key)
	if errC != nil {
		return []byte{}, errC
	}

	gcm, errN := cipher.NewGCM(c)
	if errN != nil {
		return []byte{}, errN
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return []byte{}, xerrors.Errorf("Ciphertext smaller than nonce size")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return []byte{}, err
	}
	return plaintext, nil
}

// encryptAESGCM encrypts the ciphertext in AES GCM mode
func encryptAESGCM(key []byte, plaintext []byte) ([]byte, error) {
	c, errC := aes.NewCipher(key)
	if errC != nil {
		return []byte{}, errC
	}

	gcm, errN := cipher.NewGCM(c)
	if errN != nil {
		return []byte{}, errN
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, errN = io.ReadFull(rand.Reader, nonce); errN != nil {
		return []byte{}, errN
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// generateNewAESKey generates new AES key of size AESKEYSIZE
func generateNewAESKey() ([]byte, error) {
	key := make([]byte, AESKEYSIZE)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return []byte{}, err
	}
	return key, nil
}

func remove[T comparable](l []T, item T) []T {
	out := make([]T, 0)
	for _, element := range l {
		if element != item {
			out = append(out, element)
		}
	}
	return out
}

// PROJECT HELPER FUNCTIONS
func serializeStreamMsg(msg types.StreamMessage) []byte {
	var str string
	str = str + msg.StreamInfo.StreamID
	str = str + peer.MetafileSep

	str = str + msg.StreamInfo.Name
	str = str + peer.MetafileSep

	str = str + strconv.Itoa(int(msg.StreamInfo.Price))
	str = str + peer.MetafileSep

	str = str + fmt.Sprintf("%f", msg.StreamInfo.Grade)
	str = str + peer.MetafileSep

	str = str + strconv.Itoa(int(msg.StreamInfo.CurrentlyWatching))
	str = str + peer.MetafileSep

	str = str + hex.EncodeToString(msg.Data.Chunk)
	str = str + peer.MetafileSep

	str = str + strconv.Itoa(int(msg.Data.StartIndex))
	str = str + peer.MetafileSep

	str = str + strconv.Itoa(int(msg.Data.EndIndex))
	return []byte(str)
}

func deserializeStreamMsg(serialized string) (types.StreamMessage, error) {

	streamInfo := types.StreamInfo{}

	splits := strings.Split(serialized, peer.MetafileSep)

	streamInfo.StreamID = splits[0]
	streamInfo.Name = splits[1]
	price, errPrice := strconv.Atoi(splits[2])
	if errPrice != nil {
		return types.StreamMessage{}, errPrice
	}
	streamInfo.Price = uint(price)

	grade, errGrade := strconv.ParseFloat(splits[3], 64)
	if errGrade != nil {
		return types.StreamMessage{}, errGrade
	}
	streamInfo.Grade = grade

	currWatch, errCurrW := strconv.Atoi(splits[4])
	if errCurrW != nil {
		return types.StreamMessage{}, errCurrW
	}
	streamInfo.CurrentlyWatching = uint(currWatch)

	chunk, errChunk := hex.DecodeString(splits[5])
	if errChunk != nil {
		return types.StreamMessage{}, errChunk
	}

	startIndex, errSI := strconv.Atoi(splits[6])
	if errSI != nil {
		return types.StreamMessage{}, errSI
	}
	endIndex, errEI := strconv.Atoi(splits[7])
	if errEI != nil {
		return types.StreamMessage{}, errEI
	}

	streamData := types.StreamData{
		StartIndex: uint(startIndex),
		EndIndex:   uint(endIndex),
		Chunk:      chunk,
	}
	return types.StreamMessage{
		StreamInfo: streamInfo,
		Data:       streamData,
	}, nil
}

func convertStreamMsgToPayload(streamMsg types.StreamMessage, key []byte) (string, error) {
	serialized := serializeStreamMsg(streamMsg)
	encMsg, err := encryptAESGCM(key, serialized)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(encMsg), nil
}

func convertPayloadToStreamMsg(payload string, key []byte) (types.StreamMessage, error) {

	payloadBytes, errD := hex.DecodeString(payload)
	if errD != nil {
		return types.StreamMessage{}, errD
	}

	decrypted, errDec := decryptAESGCM(key, payloadBytes)
	if errDec != nil {
		return types.StreamMessage{}, errDec
	}
	return deserializeStreamMsg(string(decrypted))
}

func sortStreamMessages(msgs []types.StreamMessage) []types.StreamMessage {
	sort.Slice(msgs, func(i, j int) bool {
		return msgs[i].Data.StartIndex < msgs[j].Data.StartIndex
	})
	return msgs
}

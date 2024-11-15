package impl

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"strconv"
	"strings"
)

type PKIInfo struct {
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
}

type ChainBlock struct {
	senderPublicKey   *rsa.PublicKey
	senderAddress     string
	receiverPublicKey *rsa.PublicKey
	receiverAddress   string
	amount            float64
	streamID          string
}

func generatePublicPrivateKey() (*rsa.PublicKey, *rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	return &privateKey.PublicKey, privateKey, nil
}

func publicKeyToString(publicKey *rsa.PublicKey) string {
	x509EncodedPub, _ := x509.MarshalPKIXPublicKey(publicKey)
	pemEncodedPub := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: x509EncodedPub})
	return string(pemEncodedPub)
}

func stringToPublicKey(pemEncodedPub string) *rsa.PublicKey {
	blockPub, _ := pem.Decode([]byte(pemEncodedPub))
	x509EncodedPub := blockPub.Bytes
	genericPublicKey, _ := x509.ParsePKIXPublicKey(x509EncodedPub)
	return genericPublicKey.(*rsa.PublicKey)
}

func stringifyChainBlock(block ChainBlock) string {
	blockStr := ""
	blockStr += block.senderAddress
	blockStr += "\t"
	blockStr += publicKeyToString(block.senderPublicKey)
	blockStr += "\t"
	blockStr += block.receiverAddress
	blockStr += "\t"
	blockStr += publicKeyToString(block.receiverPublicKey)
	blockStr += "\t"
	blockStr += block.streamID
	blockStr += "\t"
	blockStr += strconv.FormatFloat(block.amount, 'E', -1, 64)
	blockStr += "\t"
	return blockStr
}

func chainBlockFromString(blockString string) ChainBlock {
	var block ChainBlock
	for i, part := range strings.Split(blockString, "\t") {
		switch i {
		case 0:
			block.senderAddress = part
		case 1:
			block.senderPublicKey = stringToPublicKey(part)
		case 2:
			block.receiverAddress = part
		case 3:
			block.receiverPublicKey = stringToPublicKey(part)
		case 4:
			block.streamID = part
		case 5:
			block.amount, _ = strconv.ParseFloat(part, 64)
		}
	}
	return block
}

func stringifyTransport(msg transport.Message) string {
	blockStr := ""
	blockStr += msg.Type
	blockStr += "\t"
	blockStr += string(msg.Payload)
	return blockStr
}

func (n *node) PutInitialBlockOnChain(publicKey *rsa.PublicKey, address string, amount float64) error {
	block := ChainBlock{
		senderPublicKey:   publicKey,
		senderAddress:     address,
		receiverPublicKey: publicKey,
		receiverAddress:   address,
		amount:            amount,
		streamID:          "нема још",
	}
	go func() {
		_ = n.Tag(xid.New().String(), stringifyChainBlock(block))
	}()
	return nil
}

func (n *node) PkiInit(address string, amount float64) (*rsa.PublicKey, *rsa.PrivateKey, error) {
	publicKey, privateKey, err := generatePublicPrivateKey()
	if err != nil {
		return nil, nil, err
	}
	err = n.PutInitialBlockOnChain(publicKey, address, amount)
	if err != nil {
		return nil, nil, err
	}
	n.conf.MessageRegistry.RegisterMessageCallback(types.ConfidentialityMessage{}, n.ProcessConfidentialityMessage)
	return publicKey, privateKey, nil
}

func encryptMsg(plainMsg []byte, publicKey *rsa.PublicKey) ([]byte, error) {
	cipherMsg, err := rsa.EncryptOAEP(sha512.New(), rand.Reader, publicKey, plainMsg, nil)
	if err != nil {
		return nil, err
	}
	return cipherMsg, nil
}

func decryptMsg(cipherMsg []byte, privateKey *rsa.PrivateKey) ([]byte, error) {
	plainMsg, err := rsa.DecryptOAEP(sha512.New(), rand.Reader, privateKey, cipherMsg, nil)
	if err != nil {
		return nil, err
	}
	return plainMsg, nil
}

func (n *node) EncryptMsg(msg transport.Message, key *rsa.PublicKey) (*types.ConfidentialityMessage, error) {
	s := stringifyTransport(msg)
	cipherMsg, err := encryptMsg([]byte(s), key)

	log.Info().Msgf("encrypt msg unutra ciphermsg %x %s", cipherMsg, err)
	if err != nil {
		return nil, err
	}

	return &types.ConfidentialityMessage{CipherMessage: cipherMsg}, nil
}

func (n *node) CreateConfidentialityMsg(msg transport.Message, publicKey *rsa.PublicKey) (
	*types.ConfidentialityMessage,
	error,
) {
	buf, err := json.Marshal(&msg)
	if err != nil {
		return nil, err
	}

	key, err := generateNewAESKey()
	if err != nil {
		return nil, err
	}

	aesgcm, err := encryptAESGCM(key, buf)
	if err != nil {
		return nil, err
	}

	encKey, err := encryptMsg(key, publicKey)
	if err != nil {
		return nil, err
	}
	msgToSend := types.ConfidentialityMessage{
		CipherMessage: []byte(
			hex.EncodeToString(aesgcm) + peer.MetafileSep +
				hex.EncodeToString(encKey)),
	}

	return &msgToSend, nil
}

func (n *node) SendEncryptedMsg(msg transport.Message, dest string) ([]byte, error) {
	publicKey, err := n.GetPublicKey(dest)
	if err != nil {
		return nil, err
	}

	buf, err := json.Marshal(&msg)
	if err != nil {
		return nil, err
	}

	key, err := generateNewAESKey()
	if err != nil {
		return nil, err
	}

	aesgcm, err := encryptAESGCM(key, buf)
	if err != nil {
		return nil, err
	}

	encKey, err := encryptMsg(key, publicKey)
	if err != nil {
		return nil, err
	}
	msgToSend := types.ConfidentialityMessage{
		CipherMessage: []byte(
			hex.EncodeToString(aesgcm) + peer.MetafileSep +
				hex.EncodeToString(encKey)),
	}

	transportMsg, err := n.conf.MessageRegistry.MarshalMessage(msgToSend)
	if err != nil {
		return nil, err
	}

	err = n.Unicast(dest, transportMsg)
	if err != nil {
		return nil, err
	}

	//Maybe just add chat message idk?
	//Maybe it is ok to just call process message on all message types?
	if msg.Type == (types.ChatMessage{}).Name() {
		address := n.conf.Socket.GetAddress()
		localHeader := transport.NewHeader(address, address, dest, TTL)
		localPkt := transport.Packet{
			Header: &localHeader,
			Msg:    &msg,
		}
		err = n.conf.MessageRegistry.ProcessPacket(localPkt)
		if err != nil {
			return nil, err
		}
	}

	return msgToSend.CipherMessage, nil
}

func (n *node) DecryptedMsg(cipherMsg []byte, privateKey *rsa.PrivateKey) ([]byte, error) {
	tmps := strings.Split(string(cipherMsg), peer.MetafileSep)
	aesgcm, err := hex.DecodeString(tmps[0])
	if err != nil {
		//log.Error().Msgf("hex DEC 1: %s", err.Error())
		return nil, err
	}
	encKey, err := hex.DecodeString(tmps[1])
	if err != nil {
		//log.Error().Msgf("hex DEC2: %s", err.Error())
		return nil, err
	}
	key, err := decryptMsg(encKey, privateKey)
	if err != nil {
		//log.Error().Msgf("RSA DEC: %s", err.Error())
		return nil, err
	}
	bytes, err := decryptAESGCM(key, aesgcm)
	if err != nil {
		//log.Error().Msgf("gcm DEC: %s", err.Error())
		return nil, err
	}
	return bytes, nil
}

func (n *node) ProcessConfidentialityMessage(msg types.Message, pkt transport.Packet) error {
	var confMsg types.ConfidentialityMessage
	err := json.Unmarshal(pkt.Msg.Payload, &confMsg)
	if err != nil {
		log.Error().Msgf("err unmarshall: %s", err.Error())
		return err
	}

	decryptedMsg, err := n.DecryptedMsg(confMsg.CipherMessage, n.pkiInfo.privateKey)
	if err != nil {
		return err
	}
	var transportMsg transport.Message
	err = json.Unmarshal(decryptedMsg, &transportMsg)
	if err != nil {
		log.Error().Msgf("unmarsall 2 DEC: %s", err.Error())
		return err
	}
	// Process the rumor locally
	var header = transport.NewHeader(
		n.conf.Socket.GetAddress(),
		n.conf.Socket.GetAddress(),
		n.conf.Socket.GetAddress(),
		0,
	)

	// CAUTION! HERE WE PROCESS MESSAGE FROM FUNCTION ARGUMENT, NOT THE RumorMessage
	//n.activeThreads.Add(1)
	func() {
		//defer n.activeThreads.Done()
		var pkt = transport.Packet{Header: &header, Msg: &transportMsg}
		err := n.conf.MessageRegistry.ProcessPacket(pkt)
		if err != nil {
			log.Error().Msgf("%s: BROADCAST: Local message handling failed", n.conf.Socket.GetAddress())
		}
	}()
	//n.conf.MessageRegistry.ProcessPacket(transportMsg)
	//log.Printf("plaintext: %s", plaintext)
	return nil
}

func (n *node) GetPublicKey(address string) (*rsa.PublicKey, error) {
	BlockchainStore := n.conf.Storage.GetBlockchainStore()
	lastBlockHashHex := hex.EncodeToString(BlockchainStore.Get(storage.LastBlockKey))
	endBlockHasHex := hex.EncodeToString(make([]byte, 32))
	var block types.BlockchainBlock
	for lastBlockHashHex != endBlockHasHex {
		lastBlockBuf := BlockchainStore.Get(lastBlockHashHex)
		if lastBlockBuf == nil {
			return nil, xerrors.New("Last block buffer not found")
		}
		err := block.Unmarshal(lastBlockBuf)
		if err != nil {
			return nil, err
		}
		chainBlock := chainBlockFromString(block.Value.Metahash)
		if chainBlock.senderAddress == address {
			return chainBlock.senderPublicKey, nil
		}
		if chainBlock.receiverAddress == address {
			return chainBlock.receiverPublicKey, nil
		}
		lastBlockHashHex = hex.EncodeToString(block.PrevHash)
	}
	return nil, xerrors.New("End of chain: " + address + " not found")
}

func (n *node) GetAmount(address string) (float64, error) {
	errAmount := -1.0
	amount := 0.0
	BlockchainStore := n.conf.Storage.GetBlockchainStore()
	lastBlockHashHex := hex.EncodeToString(BlockchainStore.Get(storage.LastBlockKey))
	endBlockHasHex := hex.EncodeToString(make([]byte, 32))
	var block types.BlockchainBlock
	for lastBlockHashHex != endBlockHasHex {
		lastBlockBuf := BlockchainStore.Get(lastBlockHashHex)
		if lastBlockBuf == nil {
			return errAmount, xerrors.New("Last block buffer not found")
		}
		err := block.Unmarshal(lastBlockBuf)
		if err != nil {
			return errAmount, err
		}
		chainBlock := chainBlockFromString(block.Value.Metahash)
		if chainBlock.receiverAddress == address {
			amount += chainBlock.amount
		} else if chainBlock.senderAddress == address {
			amount -= chainBlock.amount
		}
		lastBlockHashHex = hex.EncodeToString(block.PrevHash)
	}
	return amount, nil
}

func (n *node) PaySubscription(senderAddress, receiverAddress, streamID string, amount float64) error {
	senderPublicKey, err := n.GetPublicKey(senderAddress)
	if err != nil {
		return err
	}
	receiverPublicKey, err := n.GetPublicKey(receiverAddress)
	if err != nil {
		return err
	}
	// check money
	currentAmount, err := n.GetAmount(senderAddress)
	if err != nil {
		return err
	}
	if currentAmount < amount {
		return xerrors.New("Not enough money")
	}
	block := ChainBlock{
		senderPublicKey:   senderPublicKey,
		senderAddress:     senderAddress,
		receiverPublicKey: receiverPublicKey,
		receiverAddress:   receiverAddress,
		amount:            amount,
		streamID:          streamID,
	}
	mh := stringifyChainBlock(block)
	return n.Tag(xid.New().String(), mh)
}

func (n *node) PaySubscriptionFull(
	senderAddress string, senderPublicKey *rsa.PublicKey,
	receiverAddress string, receiverPublicKey *rsa.PublicKey, streamID string, amount float64,
) error {
	// check money
	currentAmount, err := n.GetAmount(senderAddress)
	if err != nil {
		return err
	}
	if currentAmount < amount {
		return xerrors.New("Not enough money")
	}
	block := ChainBlock{
		senderPublicKey:   senderPublicKey,
		senderAddress:     senderAddress,
		receiverPublicKey: receiverPublicKey,
		receiverAddress:   receiverAddress,
		amount:            amount,
		streamID:          streamID,
	}
	mh := stringifyChainBlock(block)
	return n.Tag(xid.New().String(), mh)
}

func doubleCheckPayment(
	senderAddress, lastBlockHashHex, endBlockHasHex string,
	BlockchainStore storage.Store,
) (float64, error) {
	errAmount := -1.0
	prevAmount := 0.0
	var block types.BlockchainBlock
	for lastBlockHashHex != endBlockHasHex {
		lastBlockBuf := BlockchainStore.Get(lastBlockHashHex)
		if lastBlockBuf == nil {
			return errAmount, xerrors.New("Last block buffer not found")
		}
		err := block.Unmarshal(lastBlockBuf)
		if err != nil {
			return errAmount, err
		}
		chainBlock := chainBlockFromString(block.Value.Metahash)
		if chainBlock.receiverAddress == senderAddress {
			prevAmount += chainBlock.amount
		} else if chainBlock.senderAddress == senderAddress {
			prevAmount -= chainBlock.amount
		}
		lastBlockHashHex = hex.EncodeToString(block.PrevHash)
	}
	return prevAmount, nil
}

func checkStreamAmount(chainBlock ChainBlock, senderAddress, receiverAddress, streamID string, amount float64,
	BlockchainStore storage.Store, lastBlockHashHex, endBlockHasHex string) (bool, error) {

	if !(chainBlock.senderAddress == senderAddress &&
		chainBlock.receiverAddress == receiverAddress &&
		chainBlock.streamID == streamID) {
		return false, nil
	}
	if chainBlock.amount == amount {
		// check transaction validity
		prevAmount, err := doubleCheckPayment(senderAddress, lastBlockHashHex, endBlockHasHex, BlockchainStore)
		if err != nil {
			return false, err
		}
		// check transaction validity done ===
		if prevAmount < amount {
			return false, xerrors.New("User did not have enough money but somehow paid!")
		}
		return true, nil
	}
	return false, fmt.Errorf("subscription not found, but paid: %f insted of %f", chainBlock.amount, amount)
}

func (n *node) IsPayedSubscription(senderAddress, receiverAddress, streamID string, amount float64) (bool, error) {
	BlockchainStore := n.conf.Storage.GetBlockchainStore()
	lastBlockHashHex := hex.EncodeToString(BlockchainStore.Get(storage.LastBlockKey))
	endBlockHasHex := hex.EncodeToString(make([]byte, 32))
	var block types.BlockchainBlock
	for lastBlockHashHex != endBlockHasHex {
		lastBlockBuf := BlockchainStore.Get(lastBlockHashHex)
		if lastBlockBuf == nil {
			return false, xerrors.New("Last block buffer not found")
		}
		err := block.Unmarshal(lastBlockBuf)
		if err != nil {
			return false, err
		}
		chainBlock := chainBlockFromString(block.Value.Metahash)
		flag, err := checkStreamAmount(chainBlock, senderAddress, receiverAddress, streamID, amount,
			BlockchainStore, lastBlockHashHex, endBlockHasHex)
		if err != nil {
			return false, err
		}
		if flag {
			return true, nil
		}
		lastBlockHashHex = hex.EncodeToString(block.PrevHash)
	}
	return false, xerrors.New("End of chain: Subscription not found")
}

func (n *node) SetInitBlock(publicKey *rsa.PublicKey, address string, amount float64) error {
	chainBlock := ChainBlock{
		senderPublicKey:   publicKey,
		senderAddress:     address,
		receiverPublicKey: publicKey,
		receiverAddress:   address,
		amount:            amount,
		streamID:          "нема још",
	}
	mh := stringifyChainBlock(chainBlock)
	lastBlock := n.conf.Storage.GetBlockchainStore().Get(storage.LastBlockKey)
	if lastBlock == nil {
		lastBlock = make([]byte, 32)
	}
	value := types.PaxosValue{UniqID: xid.New().String(), Filename: xid.New().String(), Metahash: mh}
	index := uint(0)
	h := sha256.New()
	h.Write([]byte(strconv.Itoa(int(index))))
	h.Write([]byte(value.UniqID))
	h.Write([]byte(value.Filename))
	h.Write([]byte(value.Metahash))
	h.Write(lastBlock)
	block := types.BlockchainBlock{Index: index, Hash: h.Sum(nil), Value: value, PrevHash: lastBlock}
	blockMarshal, err := block.Marshal()
	if err != nil {
		log.Printf("Error Marshal: %s", err)
		return err
	}
	BlockchainStore := n.conf.Storage.GetBlockchainStore()
	BlockchainStore.Set(hex.EncodeToString(block.Hash), blockMarshal)
	BlockchainStore.Set(storage.LastBlockKey, block.Hash)
	return nil
}

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
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"strconv"
	"strings"
)

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
	publicKey := genericPublicKey.(*rsa.PublicKey)
	return publicKey
}

func stringifyChainBlock(block ChainBlock) (string, error) {
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
	return blockStr, nil
}

func chainBlockFromString(blockString string) (ChainBlock, error) {
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
	return block, nil
}

func stringifyTransport(msg transport.Message) (string, error) {
	blockStr := ""
	blockStr += msg.Type
	blockStr += "\t"
	blockStr += string(msg.Payload)
	return blockStr, nil
}

func transportFromString(blockString string) (transport.Message, error) {
	var msg transport.Message
	for i, part := range strings.Split(blockString, "\t") {
		switch i {
		case 0:
			msg.Type = part
		case 1:
			msg.Payload = []byte(part)
		}
	}
	return msg, nil
}

func (n *node) PutInitialBlockOnChain(publicKey *rsa.PublicKey, address string, amount float64) error {
	block := ChainBlock{
		senderPublicKey:   publicKey,
		senderAddress:     address,
		receiverPublicKey: publicKey,
		receiverAddress:   address,
		amount:            amount,
		streamID:          "нема још"}
	mh, err := stringifyChainBlock(block)
	if err != nil {
		return err
	}
	go n.Tag(xid.New().String(), mh)
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
	s, err := stringifyTransport(msg)
	log.Info().Msgf("encrypt msg unutra %s", s)
	if err != nil {
		return nil, err
	}
	cipherMsg, err := encryptMsg([]byte(s), key)

	log.Info().Msgf("encrypt msg unutra ciphermsg %x %s", cipherMsg, err)
	if err != nil {
		return nil, err
	}

	return &types.ConfidentialityMessage{CipherMessage: cipherMsg}, nil
}
func (n *node) SendEncryptedMsg(msg transport.Message, publicKey *rsa.PublicKey) ([]byte, error) {
	msgToSend, err := n.EncryptMsg(msg, publicKey)
	if err != nil {
		return nil, err
	}
	buf, err := json.Marshal(&msgToSend)
	if err != nil {
		return nil, err
	}
	return msgToSend.CipherMessage, n.Broadcast(transport.Message{Type: msgToSend.Name(), Payload: buf})
}

func (n *node) ProcessConfidentialityMessage(msg types.Message, pkt transport.Packet) error {
	var confMsg types.ConfidentialityMessage
	err := json.Unmarshal(pkt.Msg.Payload, &confMsg)
	if err != nil {
		return err
	}
	plainMsg, err := decryptMsg(confMsg.CipherMessage, n.privateKey)
	if err != nil {
		return err
	}
	// process plaintext further
	// iz bajta u transport i process
	transportMsg, err := transportFromString(string(plainMsg))
	if err != nil {
		return err
	}
	// Process the rumor locally
	var header = transport.NewHeader(
		n.conf.Socket.GetAddress(),
		n.conf.Socket.GetAddress(),
		n.conf.Socket.GetAddress(),
		0)

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
		chainBlock, err := chainBlockFromString(block.Value.Metahash)
		if err != nil {
			return nil, err
		}
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
		chainBlock, err := chainBlockFromString(block.Value.Metahash)
		if err != nil {
			return errAmount, err
		}
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
		streamID:          streamID}
	mh, err := stringifyChainBlock(block)
	if err != nil {
		return err
	}
	return n.Tag(xid.New().String(), mh)
}

func (n *node) PaySubscriptionFull(senderAddress string, senderPublicKey *rsa.PublicKey,
	receiverAddress string, receiverPublicKey *rsa.PublicKey, streamID string, amount float64) error {
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
		streamID:          streamID}
	mh, err := stringifyChainBlock(block)
	if err != nil {
		return err
	}
	return n.Tag(xid.New().String(), mh)
}

func doubleCheckPayment(senderAddress, lastBlockHashHex, endBlockHasHex string,
	BlockchainStore storage.Store) (float64, error) {
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
		chainBlock, err := chainBlockFromString(block.Value.Metahash)
		if err != nil {
			return errAmount, err
		}
		if chainBlock.receiverAddress == senderAddress {
			prevAmount += chainBlock.amount
		} else if chainBlock.senderAddress == senderAddress {
			prevAmount -= chainBlock.amount
		}
		lastBlockHashHex = hex.EncodeToString(block.PrevHash)
	}
	return prevAmount, nil
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
		chainBlock, err := chainBlockFromString(block.Value.Metahash)
		if err != nil {
			return false, err
		}
		if chainBlock.senderAddress == senderAddress &&
			chainBlock.receiverAddress == receiverAddress &&
			chainBlock.streamID == streamID {

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
			} else {
				return false, fmt.Errorf("subscription not found, but paid: %f insted of %f", chainBlock.amount, amount)
			}
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
		streamID:          "нема још"}
	mh, err := stringifyChainBlock(chainBlock)
	if err != nil {
		return err
	}
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

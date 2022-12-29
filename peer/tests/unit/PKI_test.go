package unit

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha512"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/transport/channel"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
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

func putInitialBlockOnChain(n *z.TestNode, publicKey *rsa.PublicKey, address string, amount float64) error {
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
	return n.Tag(xid.New().String(), mh)
}

func getPublicKey(n *z.TestNode, address string) (*rsa.PublicKey, error) {
	BlockchainStore := n.GetStorage().GetBlockchainStore()
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

func getAmount(n *z.TestNode, address string) (float64, error) {
	errAmount := -1.0
	amount := 0.0
	BlockchainStore := n.GetStorage().GetBlockchainStore()
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

func paySubscription(n *z.TestNode, senderAddress, receiverAddress, streamID string, amount float64) error {
	senderPublicKey, err := getPublicKey(n, senderAddress)
	if err != nil {
		return err
	}
	receiverPublicKey, err := getPublicKey(n, receiverAddress)
	if err != nil {
		return err
	}
	// check money
	currentAmount, err := getAmount(n, senderAddress)
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

func isPayedSubscription(n *z.TestNode, senderAddress, receiverAddress, streamID string, amount float64) (bool, error) {
	BlockchainStore := n.GetStorage().GetBlockchainStore()
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

func encryptMsg(plaintext string, publicKey *rsa.PublicKey) (string, error) {
	ciphertext, err := rsa.EncryptOAEP(sha512.New(), rand.Reader, publicKey, []byte(plaintext), nil)
	if err != nil {
		return "", err
	}
	return string(ciphertext), nil
}

func decryptMsg(ciphertext string, privateKey *rsa.PrivateKey) (string, error) {
	log.Info().Msgf("msg to decrypt is %s and private key %x", ciphertext, privateKey)
	plaintext, err := rsa.DecryptOAEP(sha512.New(), rand.Reader, privateKey, []byte(ciphertext), nil)
	if err != nil {
		log.Error().Msgf("nac error in decrypt msg %s", err)
		return "", err
	}
	return string(plaintext), nil
}

func sendEncryptedMsg(n *z.TestNode, msg string, publicKey *rsa.PublicKey) (string, error) {
	ciphertext, err := encryptMsg(msg, publicKey)
	if err != nil {
		return "", nil
	}
	msgToSend := types.ConfidentialityMessage{CipherMessage: ciphertext}
	buf, err := json.Marshal(&msgToSend)
	if err != nil {
		return "", err
	}
	return ciphertext, n.Broadcast(transport.Message{Type: msgToSend.Name(), Payload: buf})
}

// project tests
func Test_Project_PKI_Init_Blockchain_With_Amount(t *testing.T) {
	numNodes := 3
	amount := 100.0
	transp := channel.NewTransport()
	nodes := make([]z.TestNode, numNodes)
	for i, _ := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(uint(numNodes)), z.WithPaxosID(uint(i+1)), z.WithAntiEntropy(time.Second))
		defer node.Stop()
		nodes[i] = node
		log.Printf("node %d finish init", i)
	}
	for _, n1 := range nodes {
		for _, n2 := range nodes {
			n1.AddPeer(n2.GetAddr())
		}
	}
	log.Printf("node peers added")
	t_time := time.Duration(5)
	log.Printf("%d sec start", t_time)
	time.Sleep(time.Second * t_time)
	log.Printf("%d sec end", t_time)

	for i, node := range nodes {
		t.Logf("node %d", i)

		store := node.GetStorage().GetBlockchainStore()
		require.Equal(t, numNodes+1, store.Len())

		nodeAmount, err := getAmount(&node, node.GetAddr())
		require.NoError(t, err)
		require.Equal(t, amount, nodeAmount)
	}

}

func Test_Project_Init_Blockchain_With_Amount(t *testing.T) {
	numNodes := 3
	amount := 100.0
	transp := channel.NewTransport()
	nodes := make([]z.TestNode, numNodes)
	for i, _ := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(uint(numNodes)), z.WithPaxosID(uint(i+1)))
		defer node.Stop()
		nodes[i] = node
	}
	for _, n1 := range nodes {
		for _, n2 := range nodes {
			n1.AddPeer(n2.GetAddr())
		}
	}
	wait := sync.WaitGroup{}
	wait.Add(numNodes)

	for _, node := range nodes {
		go func(n z.TestNode, receiverAddress string) {
			defer wait.Done()
			publicKey, _, err := generatePublicPrivateKey()
			require.NoError(t, err)
			err = putInitialBlockOnChain(&n, publicKey, n.GetAddr(), amount)
			require.NoError(t, err)

			time.Sleep(time.Second)
		}(node, nodes[0].GetAddr())
	}

	wait.Wait()

	time.Sleep(time.Second * 2)

	for i, node := range nodes {
		t.Logf("node %d", i)

		store := node.GetStorage().GetBlockchainStore()
		require.Equal(t, numNodes+1, store.Len())

		nodeAmount, err := getAmount(&node, node.GetAddr())
		require.NoError(t, err)
		require.Equal(t, amount, nodeAmount)
	}

}

func Test_Project_Subscription_Enough_Money(t *testing.T) {
	numNodes := 3
	amount := 100.0
	cost := 20.0
	transp := channel.NewTransport()

	nodes := make([]z.TestNode, numNodes)

	for i, _ := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(uint(numNodes)), z.WithPaxosID(uint(i+1)))
		defer node.Stop()
		nodes[i] = node
	}
	for _, n1 := range nodes {
		for _, n2 := range nodes {
			n1.AddPeer(n2.GetAddr())
		}
	}
	wait := sync.WaitGroup{}
	wait.Add(numNodes)

	for _, node := range nodes {
		go func(n z.TestNode, receiverAddress string) {
			defer wait.Done()
			publicKey, _, err := generatePublicPrivateKey()
			require.NoError(t, err)
			err = putInitialBlockOnChain(&n, publicKey, n.GetAddr(), amount)
			require.NoError(t, err)

			time.Sleep(time.Second)
			log.Printf("block put on chain %s", n.GetAddr())
		}(node, nodes[0].GetAddr())
	}

	log.Printf("wait check")
	wait.Wait()
	log.Printf("sleep 2s")
	time.Sleep(time.Second * 2)
	log.Printf("sleep 2s pass")
	wait = sync.WaitGroup{}
	wait.Add(numNodes - 1)

	for i, node := range nodes {
		if i != 0 {
			go func(n z.TestNode, receiverAddress string) {
				defer wait.Done()

				store := node.GetStorage().GetBlockchainStore()
				require.Equal(t, numNodes+1, store.Len())

				nodeAmount, err := getAmount(&node, node.GetAddr())
				require.NoError(t, err)
				require.Equal(t, amount, nodeAmount)

				err = paySubscription(&n, n.GetAddr(), receiverAddress, "снимак", cost)
				require.NoError(t, err)
				time.Sleep(time.Second)
				log.Printf("subscription paid %s", n.GetAddr())
			}(node, nodes[0].GetAddr())
		}
	}

	wait.Wait()

	time.Sleep(time.Second * 2)

	for i, node := range nodes {
		t.Logf("node %d", i)
		store := node.GetStorage().GetBlockchainStore()
		require.Equal(t, numNodes+1+(numNodes-1), store.Len())
		nodeAmount, err := getAmount(&node, node.GetAddr())
		require.NoError(t, err)
		if i == 0 {
			require.Equal(t, amount+float64(numNodes-1)*cost, nodeAmount)
		} else {
			require.Equal(t, amount-cost, nodeAmount)
		}
	}
}

func Test_Project_Subscription_Not_Enough_Money(t *testing.T) {
	numNodes := 2
	amount := 100.0
	cost := 120.0
	transp := channel.NewTransport()

	nodes := make([]z.TestNode, numNodes)

	for i, _ := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(uint(numNodes)), z.WithPaxosID(uint(i+1)))
		defer node.Stop()
		nodes[i] = node
	}
	for _, n1 := range nodes {
		for _, n2 := range nodes {
			n1.AddPeer(n2.GetAddr())
		}
	}
	wait := sync.WaitGroup{}
	wait.Add(numNodes)

	for _, node := range nodes {
		go func(n z.TestNode, receiverAddress string) {
			defer wait.Done()
			publicKey, _, err := generatePublicPrivateKey()
			require.NoError(t, err)
			err = putInitialBlockOnChain(&n, publicKey, n.GetAddr(), amount)
			require.NoError(t, err)

			time.Sleep(time.Second)
			log.Printf("block put on chain %s", n.GetAddr())
		}(node, nodes[0].GetAddr())
	}

	log.Printf("wait check")
	wait.Wait()
	log.Printf("sleep 2s")
	time.Sleep(time.Second * 2)
	log.Printf("sleep 2s pass")
	wait = sync.WaitGroup{}
	wait.Add(numNodes - 1)

	for i, node := range nodes {
		if i != 0 {
			go func(n z.TestNode, receiverAddress string) {
				defer wait.Done()

				store := node.GetStorage().GetBlockchainStore()
				require.Equal(t, numNodes+1, store.Len())

				nodeAmount, err := getAmount(&node, node.GetAddr())
				require.NoError(t, err)
				require.Equal(t, amount, nodeAmount)

				err = paySubscription(&n, n.GetAddr(), receiverAddress, "снимак", cost)
				require.Error(t, err)
				time.Sleep(time.Second)
				log.Printf("subscription was not paid %s", n.GetAddr())
			}(node, nodes[0].GetAddr())
		}
	}

	wait.Wait()

	time.Sleep(time.Second * 2)

	for i, node := range nodes {
		t.Logf("node %d", i)
		store := node.GetStorage().GetBlockchainStore()
		require.Equal(t, numNodes+1, store.Len())
		nodeAmount, err := getAmount(&node, node.GetAddr())
		require.NoError(t, err)
		require.Equal(t, amount, nodeAmount)
	}
}

func Test_Project_Subscription_Check(t *testing.T) {
	numNodes := 3
	amount := 100.0
	cost := 20.0
	transp := channel.NewTransport()

	nodes := make([]z.TestNode, numNodes)

	for i, _ := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(uint(numNodes)), z.WithPaxosID(uint(i+1)))
		defer node.Stop()
		nodes[i] = node
	}
	for _, n1 := range nodes {
		for _, n2 := range nodes {
			n1.AddPeer(n2.GetAddr())
		}
	}
	wait := sync.WaitGroup{}
	wait.Add(numNodes)

	for _, node := range nodes {
		go func(n z.TestNode, receiverAddress string) {
			defer wait.Done()
			publicKey, _, err := generatePublicPrivateKey()
			require.NoError(t, err)
			err = putInitialBlockOnChain(&n, publicKey, n.GetAddr(), amount)
			require.NoError(t, err)

			time.Sleep(time.Second)
			log.Printf("block put on chain %s", n.GetAddr())
		}(node, nodes[0].GetAddr())
	}

	log.Printf("wait check")
	wait.Wait()
	log.Printf("sleep 2s")
	time.Sleep(time.Second * 2)
	log.Printf("sleep 2s pass")
	wait = sync.WaitGroup{}
	wait.Add(numNodes - 1)

	for i, node := range nodes {
		if i != 0 {
			go func(n z.TestNode, receiverAddress string) {
				defer wait.Done()

				store := node.GetStorage().GetBlockchainStore()
				require.Equal(t, numNodes+1, store.Len())

				nodeAmount, err := getAmount(&node, node.GetAddr())
				require.NoError(t, err)
				require.Equal(t, amount, nodeAmount)

				err = paySubscription(&n, n.GetAddr(), receiverAddress, "снимак", cost)
				require.NoError(t, err)
				time.Sleep(time.Second)
				log.Printf("subscription paid %s", n.GetAddr())
			}(node, nodes[0].GetAddr())
		}
	}

	wait.Wait()

	time.Sleep(time.Second * 2)

	for i, node := range nodes {
		t.Logf("node %d", i)
		store := node.GetStorage().GetBlockchainStore()
		require.Equal(t, numNodes+1+(numNodes-1), store.Len())
		nodeAmount, err := getAmount(&node, node.GetAddr())
		require.NoError(t, err)
		if i == 0 {
			require.Equal(t, amount+float64(numNodes-1)*cost, nodeAmount)
		} else {
			require.Equal(t, amount-cost, nodeAmount)
			subscription, err := isPayedSubscription(&node, node.GetAddr(), nodes[0].GetAddr(), "снимак", cost)
			require.NoError(t, err)
			require.Equal(t, true, subscription)
		}
	}
}

func Test_Project_Message_Encryption_Decryption(t *testing.T) {
	numNodes := 2
	amount := 100.0
	transp := channel.NewTransport()

	nodes := make([]z.TestNode, numNodes)

	for i, _ := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(uint(numNodes)), z.WithPaxosID(uint(i+1)))
		defer node.Stop()
		log.Printf("i: %d", i)
		nodes[i] = node
	}
	for _, n1 := range nodes {
		for _, n2 := range nodes {
			n1.AddPeer(n2.GetAddr())
		}
	}

	var privateKeyMy *rsa.PrivateKey
	sendIndex := 0
	for _, n := range nodes {
		publicKey, privateKey, err := generatePublicPrivateKey()
		if sendIndex == 0 {
			privateKeyMy = privateKey
			sendIndex++
		}
		require.NoError(t, err)
		err = putInitialBlockOnChain(&n, publicKey, n.GetAddr(), amount)
		require.NoError(t, err)

		time.Sleep(time.Second)
		log.Printf("block put on chain %s", n.GetAddr())
	}

	log.Printf("wait check")
	log.Printf("sleep 2s")
	time.Sleep(time.Second * 2)
	log.Printf("sleep 2s pass")

	plaintext := "НЕРФ"
	publicKey, err := getPublicKey(&nodes[1], nodes[0].GetAddr())
	require.NoError(t, err)
	ciphertext, err := sendEncryptedMsg(&nodes[1], plaintext, publicKey)
	require.NoError(t, err)
	plaintextDec, err := decryptMsg(ciphertext, privateKeyMy)
	require.NoError(t, err)
	require.Equal(t, plaintextDec, plaintext)
}

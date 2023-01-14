package unit

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha512"
	"encoding/json"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/registry/standard"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/transport/channel"
	"go.dedis.ch/cs438/types"
	"testing"
	"time"
)

// util func
func generatePublicPrivateKey() (*rsa.PublicKey, *rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	return &privateKey.PublicKey, privateKey, nil
}

func decryptMsg(cipherMsg []byte, privateKey *rsa.PrivateKey) ([]byte, error) {
	plainMsg, err := rsa.DecryptOAEP(sha512.New(), rand.Reader, privateKey, cipherMsg, nil)
	if err != nil {
		return nil, err
	}
	return plainMsg, nil
}

// project tests
func Test_Project_PKI_Init_Blockchain_With_Amount(t *testing.T) {
	numNodes := 3
	amount := 100.0
	transp := channel.NewTransport()
	nodes := make([]z.TestNode, numNodes)
	for i, _ := range nodes {
		node := z.NewTestNode(
			t,
			peerFac,
			transp,
			"127.0.0.1:0",
			z.WithTotalPeers(uint(numNodes)),
			z.WithProjectFunctionalities(true),
			z.WithPaxosID(uint(i+1)),
			z.WithAntiEntropy(time.Second),
		)
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

		nodeAmount, err := node.GetAmount(node.GetAddr())
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
		node := z.NewTestNode(
			t,
			peerFac,
			transp,
			"127.0.0.1:0",
			z.WithProjectFunctionalities(true),
			z.WithTotalPeers(uint(numNodes)),
			z.WithPaxosID(uint(i+1)),
		)
		defer node.Stop()
		nodes[i] = node
	}
	for _, n1 := range nodes {
		for _, n2 := range nodes {
			n1.AddPeer(n2.GetAddr())
		}
	}

	for _, node := range nodes {
		publicKey, _, err := generatePublicPrivateKey()
		require.NoError(t, err)
		err = node.PutInitialBlockOnChain(publicKey, node.GetAddr(), amount)
		require.NoError(t, err)

		time.Sleep(time.Second)
	}
	time.Sleep(time.Second * 2)

	for i, node := range nodes {
		t.Logf("node %d", i)
		nodeAmount, err := node.GetAmount(node.GetAddr())
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
		node := z.NewTestNode(
			t,
			peerFac,
			transp,
			"127.0.0.1:0",
			z.WithProjectFunctionalities(true),
			z.WithTotalPeers(uint(numNodes)),
			z.WithPaxosID(uint(i+1)),
		)
		defer node.Stop()
		nodes[i] = node
	}
	for _, n1 := range nodes {
		for _, n2 := range nodes {
			n1.AddPeer(n2.GetAddr())
		}
	}

	for i, node := range nodes {
		publicKey, _, err := generatePublicPrivateKey()
		require.NoError(t, err)
		log.Printf("Pusten, %d", i)
		err = node.PutInitialBlockOnChain(publicKey, node.GetAddr(), amount)
		require.NoError(t, err)

		time.Sleep(time.Second)
		log.Printf("block put on chain %s", node.GetAddr())

		store := node.GetStorage().GetBlockchainStore()
		require.Equal(t, (i+1)+1, store.Len())
	}

	log.Printf("sleep 2s")
	//time.Sleep(time.Second * 2)
	log.Printf("sleep 2s pass")
	for i, node := range nodes {
		if i != 0 {
			nodeAmount, err := node.GetAmount(node.GetAddr())
			require.NoError(t, err)
			require.Equal(t, amount, nodeAmount)

			err = node.PaySubscription(node.GetAddr(), nodes[0].GetAddr(), "снимак", cost)
			require.NoError(t, err)
			time.Sleep(time.Second)
			log.Printf("subscription paid %s", node.GetAddr())
		}
	}

	time.Sleep(time.Second * 2)

	for i, node := range nodes {
		t.Logf("node %d", i)
		store := node.GetStorage().GetBlockchainStore()
		require.Equal(t, numNodes+1+(numNodes-1), store.Len())
		nodeAmount, err := node.GetAmount(node.GetAddr())
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
		node := z.NewTestNode(
			t,
			peerFac,
			transp,
			"127.0.0.1:0",
			z.WithProjectFunctionalities(true),
			z.WithTotalPeers(uint(numNodes)),
			z.WithPaxosID(uint(i+1)),
		)
		defer node.Stop()
		nodes[i] = node
	}
	for _, n1 := range nodes {
		for _, n2 := range nodes {
			n1.AddPeer(n2.GetAddr())
		}
	}

	for _, node := range nodes {
		publicKey, _, err := generatePublicPrivateKey()
		require.NoError(t, err)
		err = node.PutInitialBlockOnChain(publicKey, node.GetAddr(), amount)
		require.NoError(t, err)

		time.Sleep(time.Second)
		log.Printf("block put on chain %s", node.GetAddr())
	}
	log.Printf("sleep 2s")
	time.Sleep(time.Second * 2)
	log.Printf("sleep 2s pass")

	for i, node := range nodes {
		if i != 0 {
			store := node.GetStorage().GetBlockchainStore()
			require.Equal(t, numNodes+1, store.Len())

			nodeAmount, err := node.GetAmount(node.GetAddr())
			require.NoError(t, err)
			require.Equal(t, amount, nodeAmount)

			err = node.PaySubscription(node.GetAddr(), nodes[0].GetAddr(), "снимак", cost)
			require.Error(t, err)
			time.Sleep(time.Second)
			log.Printf("subscription was not paid %s", node.GetAddr())
		}
	}

	time.Sleep(time.Second * 2)

	for i, node := range nodes {
		t.Logf("node %d", i)
		store := node.GetStorage().GetBlockchainStore()
		require.Equal(t, numNodes+1, store.Len())
		nodeAmount, err := node.GetAmount(node.GetAddr())
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
		node := z.NewTestNode(
			t,
			peerFac,
			transp,
			"127.0.0.1:0",
			z.WithProjectFunctionalities(true),
			z.WithTotalPeers(uint(numNodes)),
			z.WithPaxosID(uint(i+1)),
		)
		defer node.Stop()
		nodes[i] = node
	}
	for _, n1 := range nodes {
		for _, n2 := range nodes {
			n1.AddPeer(n2.GetAddr())
		}
	}

	for _, node := range nodes {
		publicKey, _, err := generatePublicPrivateKey()
		require.NoError(t, err)
		err = node.PutInitialBlockOnChain(publicKey, node.GetAddr(), amount)
		require.NoError(t, err)

		time.Sleep(time.Second)
		log.Printf("block put on chain %s", node.GetAddr())
	}
	log.Printf("sleep 2s")
	time.Sleep(time.Second * 2)
	log.Printf("sleep 2s pass")

	for i, node := range nodes {
		if i != 0 {
			nodeAmount, err := node.GetAmount(node.GetAddr())
			require.NoError(t, err)
			require.Equal(t, amount, nodeAmount)

			err = node.PaySubscription(node.GetAddr(), nodes[0].GetAddr(), "снимак", cost)
			require.NoError(t, err)
			time.Sleep(time.Second)
			log.Printf("subscription paid %s", node.GetAddr())
		}
	}
	time.Sleep(time.Second * 2)

	for i, node := range nodes {
		t.Logf("node %d", i)
		store := node.GetStorage().GetBlockchainStore()
		require.Equal(t, numNodes+1+(numNodes-1), store.Len())
		nodeAmount, err := node.GetAmount(node.GetAddr())
		require.NoError(t, err)
		if i == 0 {
			require.Equal(t, amount+float64(numNodes-1)*cost, nodeAmount)
		} else {
			require.Equal(t, amount-cost, nodeAmount)
			subscription, err := node.IsPayedSubscription(node.GetAddr(), nodes[0].GetAddr(), "снимак", cost)
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
		node := z.NewTestNode(
			t,
			peerFac,
			transp,
			"127.0.0.1:0",
			z.WithProjectFunctionalities(true),
			z.WithTotalPeers(uint(numNodes)),
			z.WithPaxosID(uint(i+1)),
		)
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
		err = n.PutInitialBlockOnChain(publicKey, n.GetAddr(), amount)
		require.NoError(t, err)

		time.Sleep(time.Second)
		log.Printf("block put on chain %s", n.GetAddr())
	}

	log.Printf("wait check")
	log.Printf("sleep 2s")
	time.Sleep(time.Second * 2)
	log.Printf("sleep 2s pass")

	msgToSend := types.ChatMessage{Message: "hello world"}
	expectedMsg, err := standard.NewRegistry().MarshalMessage(msgToSend)
	require.NoError(t, err)
	cipherMsg, err := nodes[1].SendEncryptedMsg(expectedMsg, nodes[0].GetAddr())
	require.NoError(t, err)
	plaintextDec, err := nodes[0].DecryptedMsg(cipherMsg, privateKeyMy)
	require.NoError(t, err)
	var transportMsg transport.Message
	err = json.Unmarshal(plaintextDec, &transportMsg)
	require.NoError(t, err)
	require.Equal(t, expectedMsg, transportMsg)
}

package unit

import (
	"bufio"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
	"os"
	"sync"
	"testing"
	"time"
)

func Test_Crowds_Messaging_Request(t *testing.T) {
	numNodes := 5
	transp := channel.NewTransport()

	nodes := make([]z.TestNode, numNodes)

	for i, _ := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(uint(numNodes)), z.WithPaxosID(uint(i+1)), z.WithAntiEntropy(time.Second))
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

	numTrustedPeers := 3
	trustedPeers := make([]string, numTrustedPeers)
	for i, _ := range trustedPeers {
		trustedPeers[i] = nodes[i].GetAddr()
	}
	finalNode := nodes[numNodes-1]

	time.Sleep(time.Second * 2)

	err := nodes[0].CrowdsSend(trustedPeers, "hey there :)", finalNode.GetAddr())
	require.NoError(t, err)

	time.Sleep(time.Second * 2)

	log.Info().Msgf("Izvolte rezultati: ")
	for i, _ := range nodes {
		log.Info().Msgf("rez node %x: %s %s", i, nodes[i].GetIns(), nodes[i].GetOuts())
	}

	chatMsgs := finalNode.GetChatMsgs()
	log.Info().Msgf("%s", chatMsgs)
}

// A wants to download file via crowds...
// A <-> B <-> C <-> D
func Test_Crowds_Crowds_Download_Remote_And_Local_With_relay(t *testing.T) {
	transp := channel.NewTransport()

	node0 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(4), z.WithPaxosID(1), z.WithAntiEntropy(time.Second))
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(4), z.WithPaxosID(2), z.WithAntiEntropy(time.Second))
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(4), z.WithPaxosID(3), z.WithAntiEntropy(time.Second))
	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(4), z.WithPaxosID(4), z.WithAntiEntropy(time.Second))

	defer node0.Stop()
	defer node1.Stop()
	defer node2.Stop()
	defer node3.Stop()

	node0.AddPeer(node1.GetAddr())
	node1.AddPeer(node0.GetAddr())
	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node2.GetAddr())

	node0.SetRoutingEntry(node3.GetAddr(), node1.GetAddr())
	node0.SetRoutingEntry(node2.GetAddr(), node1.GetAddr())
	node1.SetRoutingEntry(node3.GetAddr(), node2.GetAddr())
	node2.SetRoutingEntry(node0.GetAddr(), node1.GetAddr())
	node3.SetRoutingEntry(node1.GetAddr(), node2.GetAddr())
	node3.SetRoutingEntry(node0.GetAddr(), node2.GetAddr())

	// Setting a file (chunks + metahash) in the node1's storage. Chunk n°2 will
	// only be available on node 3.

	chunks := [][]byte{{'a', 'a', 'a'}, {'b', 'b', 'b'}}
	data := append(chunks[0], chunks[1]...)

	// sha256 of each chunk, computed by hand
	c1 := "9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0"
	c2 := "3e744b9dc39389baf0c5a0660589b8402f3dbb49b89b3e75f2c9355852a3c677"

	// metahash, computed by hand
	mh := "6a0b1d67884e58786e97bc51544cbba4cc3e1279d8ff46da2fa32bcdb44a053e"

	time.Sleep(time.Second * 2)

	log.Info().Msgf("krecem da sredjujem katalog i blob store")

	storage := node1.GetStorage().GetDataBlobStore()
	storage.Set(c1, chunks[0])
	storage.Set(mh, []byte(fmt.Sprintf("%s\n%s", c1, c2)))

	storage = node3.GetStorage().GetDataBlobStore()
	storage.Set(c2, chunks[1])

	// telling node1 that node3 has the data.

	node1.UpdateCatalog(c1, node1.GetAddr())
	node1.UpdateCatalog(c2, node3.GetAddr())
	node1.UpdateCatalog(string(mh), node1.GetAddr())

	node2.UpdateCatalog(c1, node1.GetAddr())
	node2.UpdateCatalog(c2, node3.GetAddr())
	node2.UpdateCatalog(string(mh), node1.GetAddr())

	node3.UpdateCatalog(c1, node1.GetAddr())
	node3.UpdateCatalog(c2, node3.GetAddr())
	node3.UpdateCatalog(string(mh), node1.GetAddr())

	node0.UpdateCatalog(c1, node1.GetAddr())
	node0.UpdateCatalog(c2, node3.GetAddr())
	node0.UpdateCatalog(string(mh), node1.GetAddr())

	numTrustedPeers := 3
	trustedPeers := make([]string, numTrustedPeers)
	trustedPeers[0] = node0.GetAddr()
	trustedPeers[1] = node1.GetAddr()
	trustedPeers[2] = node3.GetAddr()

	filename := "testFile.txt"
	node2.Tag(filename, mh)
	time.Sleep(time.Second * 3)

	log.Info().Msgf("iniciram crowds download")
	buf, err := node0.CrowdsDownload(trustedPeers, filename)
	require.NoError(t, err)
	require.Equal(t, data, buf)

	time.Sleep(time.Second * 3)

	log.Info().Msgf("Izvolte rezultati: ")
	log.Info().Msgf("rez node %x: %s %s", 0, node0.GetIns(), node0.GetOuts())
	log.Info().Msgf("rez node %x: %s %s", 1, node1.GetIns(), node1.GetOuts())
	log.Info().Msgf("rez node %x: %s %s", 2, node2.GetIns(), node2.GetOuts())
	log.Info().Msgf("rez node %x: %s %s", 3, node3.GetIns(), node3.GetOuts())
}

// A wants to download file via crowds...
// A <-> B <-> C <-> D
func Test_FILES(t *testing.T) {
	transp := channel.NewTransport()

	chunkSize := uint(8192 * 10) // The metafile can handle just 3 chunks

	node0 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(2), z.WithTotalPeers(4), z.WithAntiEntropy(time.Second))
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(3), z.WithTotalPeers(4), z.WithAntiEntropy(time.Second))
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithTotalPeers(4), z.WithPaxosID(1), z.WithAntiEntropy(time.Second))
	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(4), z.WithTotalPeers(4), z.WithAntiEntropy(time.Second))
	defer node0.Stop()
	defer node1.Stop()
	defer node2.Stop()
	defer node3.Stop()

	node0.AddPeer(node1.GetAddr())
	node1.AddPeer(node0.GetAddr())
	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node2.GetAddr())

	node0.SetRoutingEntry(node3.GetAddr(), node1.GetAddr())
	node0.SetRoutingEntry(node2.GetAddr(), node1.GetAddr())
	node1.SetRoutingEntry(node3.GetAddr(), node2.GetAddr())
	node2.SetRoutingEntry(node0.GetAddr(), node1.GetAddr())
	node3.SetRoutingEntry(node1.GetAddr(), node2.GetAddr())
	node3.SetRoutingEntry(node0.GetAddr(), node2.GetAddr())

	filename := "proba.mp4"
	file, err := os.Open(filename)
	mh, err := node2.Upload(bufio.NewReader(file))
	if err != nil {
		log.Error().Msgf("greska u uupload %s", err)
	}
	log.Info().Msgf("tagging metahash %s with name %s", mh, filename)

	node2.Tag(filename, mh)
	time.Sleep(time.Second * 5)
	mhNew := node0.Resolve(filename)
	log.Info().Msgf("metahash resolved %s vs original %s", mhNew, mh)

	storage := node2.GetStorage().GetDataBlobStore()
	storage.ForEach(func(key string, val []byte) bool {
		node0.UpdateCatalog(key, node2.GetAddr())
		node1.UpdateCatalog(key, node2.GetAddr())
		node3.UpdateCatalog(key, node2.GetAddr())
		return true
	})
	node0.UpdateCatalog(mh, node2.GetAddr())
	node1.UpdateCatalog(mh, node2.GetAddr())
	node3.UpdateCatalog(mh, node2.GetAddr())

	numTrustedPeers := 3
	trustedPeers := make([]string, numTrustedPeers)
	trustedPeers[0] = node0.GetAddr()
	trustedPeers[1] = node1.GetAddr()
	trustedPeers[2] = node3.GetAddr()

	log.Info().Msgf("pre crowds: ")
	buf, err := node0.CrowdsDownload(trustedPeers, filename)
	require.NoError(t, err)
	f, err := os.ReadFile(filename)
	require.Equal(t, f, buf)

	log.Info().Msgf("pre sleepa: ")

	time.Sleep(time.Second * 2)

	log.Info().Msgf("Izvolte rezultati: ")
	log.Info().Msgf("rez node %x: %s %s", 0, node0.GetIns(), node0.GetOuts())
	log.Info().Msgf("rez node %x: %s %s", 1, node1.GetIns(), node1.GetOuts())
	log.Info().Msgf("rez node %x: %s %s", 2, node2.GetIns(), node2.GetOuts())
	log.Info().Msgf("rez node %x: %s %s", 3, node3.GetIns(), node3.GetOuts())
}

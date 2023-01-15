package integration

import (
	"bytes"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
	"testing"
	"time"
)

// 0 - 1 - 2 - 3
// Node 0 downloads file using crowds
// Node 0 streams downloaded file
// Client react to stream
func Test_Project_Integration_Test(t *testing.T) {
	transp := channel.NewTransport()

	node0 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(4),
		z.WithContinueMongering(1), z.WithProjectFunctionalities(true),
		z.WithChunkSize(1024), z.WithPaxosID(1), z.WithAntiEntropy(20*time.Millisecond))
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(1024),
		z.WithContinueMongering(1), z.WithProjectFunctionalities(true),
		z.WithTotalPeers(4), z.WithPaxosID(2), z.WithAntiEntropy(20*time.Millisecond))
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(4),
		z.WithContinueMongering(1), z.WithProjectFunctionalities(true),
		z.WithChunkSize(1024), z.WithPaxosID(3), z.WithAntiEntropy(20*time.Millisecond))
	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(4),
		z.WithContinueMongering(1), z.WithProjectFunctionalities(true),
		z.WithChunkSize(1024), z.WithPaxosID(4), z.WithAntiEntropy(20*time.Millisecond))

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

	// Wait for the end of PKI
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

	log.Info().Msgf("initiate crowds download")
	flag, err := node0.CrowdsDownload(trustedPeers, filename)
	require.NoError(t, err)

	require.Equal(t, true, flag)

	require.Equal(t, 3, node0.GetStorage().GetDataBlobStore().Len())
	require.Equal(t, []byte{'a', 'a', 'a'}, node0.GetStorage().GetDataBlobStore().Get(c1))
	require.Equal(t, []byte{'b', 'b', 'b'}, node0.GetStorage().GetDataBlobStore().Get(c2))
	require.Equal(t, []byte(fmt.Sprintf("%s\n%s", c1, c2)), node0.GetStorage().GetDataBlobStore().Get(mh))

	// Wait for the crowds to complete
	time.Sleep(time.Second * 3)

	// Stream downloaded file
	price := 10
	streamID, err := node0.AnnounceStartStreaming(filename, uint(price), []byte{})
	require.NoError(t, err)

	// Wait for announcement to finish
	time.Sleep(time.Second)
	err = node2.ConnectToStream(streamID, node0.GetAddr())
	require.NoError(t, err)

	// Wait for joining to finish
	time.Sleep(4 * time.Second)
	// Connect nodes to a stream
	err = node3.ConnectToStream(streamID, node0.GetAddr())
	require.NoError(t, err)

	// Wait for joining to finish
	time.Sleep(4 * time.Second)

	err = node2.ReactToStream(streamID, node0.GetAddr(), 5.0)
	require.NoError(t, err)
	err = node3.ReactToStream(streamID, node0.GetAddr(), 4.0)
	require.NoError(t, err)
	time.Sleep(time.Second)
	clients, err := node0.GetClients(streamID)
	require.NoError(t, err)
	require.Len(t, clients, 2)

	// Stream
	err = node0.Stream(bytes.NewBuffer(data), filename, uint(price), streamID, []byte{}, 0)
	require.NoError(t, err)

	// Wait for stream to finish
	time.Sleep(time.Second)

	// Node 1 should not have any stream messages
	_, errNode2 := node1.GetNextChunks(streamID, -1)
	require.Error(t, errNode2)

	// Node 2 should have received chunks
	streamMsgs, errC := node2.GetNextChunks(streamID, -1)
	require.NoError(t, errC)
	leftInd := 0
	for ind, msg := range streamMsgs {
		require.Equal(t, msg.StreamInfo.Name, filename)
		require.Equal(t, msg.StreamInfo.Price, uint(price))
		require.Equal(t, msg.StreamInfo.CurrentlyWatching, uint(2))
		require.Greater(t, msg.StreamInfo.Grade, 4.4)
		require.Less(t, msg.StreamInfo.Grade, 4.6)
		require.Equal(t, msg.Data.Chunk, data)
		require.Equal(t, msg.Data.StartIndex, uint(leftInd))
		leftInd = leftInd + len(chunks[ind])
	}

	// Node 3 should have received chunks
	streamMsgs, errC = node3.GetNextChunks(streamID, -1)
	require.NoError(t, errC)
	leftInd = 0
	for ind, msg := range streamMsgs {
		require.Equal(t, msg.StreamInfo.Name, filename)
		require.Equal(t, msg.StreamInfo.Price, uint(price))
		require.Equal(t, msg.StreamInfo.CurrentlyWatching, uint(2))
		require.Greater(t, msg.StreamInfo.Grade, 4.4)
		require.Less(t, msg.StreamInfo.Grade, 4.6)
		require.Equal(t, msg.Data.Chunk, data)
		require.Equal(t, msg.Data.StartIndex, uint(leftInd))
		leftInd = leftInd + len(chunks[ind])
	}
	log.Info().Msgf("Test Done")

}

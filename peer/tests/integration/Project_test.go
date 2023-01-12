package integration

import (
	"bufio"
	"bytes"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
	"os"
	"testing"
	"time"
)

func Test_Project_Integration_Test(t *testing.T) {
	transp := channel.NewTransport()

	chunkSize := uint(8192 * 10) // The metafile can handle just 3 chunks

	nodeA := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(3),
		z.WithTotalPeers(1), z.WithAntiEntropy(time.Second))
	nodeB := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(1),
		z.WithTotalPeers(1), z.WithAntiEntropy(time.Second))

	defer nodeA.Stop()
	defer nodeB.Stop()

	nodeA.AddPeer(nodeB.GetAddr())
	nodeB.AddPeer(nodeA.GetAddr())

	filename := "proba.mp4"
	file, err := os.Open(filename)
	mh, err := nodeB.Upload(bufio.NewReader(file))
	if err != nil {
		log.Error().Msgf("greska u uupload %s", err)
	}
	log.Info().Msgf("tagging metahash %s with name %s", mh, filename)

	nodeB.Tag(filename, mh)
	time.Sleep(time.Second * 5)
	mhNew := nodeA.Resolve(filename)
	log.Info().Msgf("metahash resolved %s vs original %s", mhNew, mh)

	storage := nodeB.GetStorage().GetDataBlobStore()
	storage.ForEach(func(key string, val []byte) bool {
		nodeA.UpdateCatalog(key, nodeB.GetAddr())
		return true
	})
	nodeA.UpdateCatalog(mh, nodeB.GetAddr())

	numTrustedPeers := 1
	trustedPeers := make([]string, numTrustedPeers)
	trustedPeers[0] = nodeA.GetAddr()

	log.Info().Msgf("pre crowds: ")
	data, err := nodeA.CrowdsDownload(trustedPeers, filename)
	require.NoError(t, err)
	f, err := os.ReadFile(filename)
	require.Equal(t, f, data)

	log.Info().Msgf("pre sleepa: ")

	time.Sleep(time.Second * 2)

	log.Info().Msgf("rez node %x: %s %s", 1, nodeA.GetIns(), nodeA.GetOuts())
	log.Info().Msgf("rez node %x: %s %s", 2, nodeB.GetIns(), nodeB.GetOuts())

	// ----

	//chunk1 := make([]byte, chunkSize)
	//chunk2 := make([]byte, chunkSize)
	//chunk3 := make([]byte, chunkSize/3)
	//chunks := [][]byte{chunk1, chunk2, chunk3}
	//chunk1[0] = 0xa
	//chunk2[0] = 0xb
	//chunk3[0] = 0xc
	//
	//data := append(chunk1, append(chunk2, chunk3...)...)
	//
	//fileName := "file"
	price := 10
	//
	buf := bytes.NewBuffer(data)

	streamID, err := nodeA.AnnounceStartStreaming(filename, uint(price), []byte{})
	require.NoError(t, err)

	// Wait for announcement to finish
	time.Sleep(10 * time.Millisecond)
	err = nodeB.ConnectToStream(streamID, nodeA.GetAddr())
	require.NoError(t, err)

	// Wait for joining to finish
	time.Sleep(20 * time.Millisecond)

	err = nodeB.ReactToStream(streamID, nodeA.GetAddr(), 5.0)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	clients, err := nodeA.GetClients(streamID)
	require.NoError(t, err)
	require.Len(t, clients, 2)

	// Stream
	err = nodeA.Stream(buf, filename, uint(price), streamID, []byte{}, 0)
	require.NoError(t, err)

	// Wait for stream to finish
	time.Sleep(500 * time.Millisecond)

	// Node 2 should have received chunks
	//streamMsgs, errC := nodeB.GetNextChunks(streamID, len(chunks))
	//require.NoError(t, errC)
	//leftInd := 0
	//for ind, msg := range streamMsgs {
	//	require.Equal(t, msg.StreamInfo.Name, filename)
	//	require.Equal(t, msg.StreamInfo.Price, uint(price))
	//	require.Equal(t, msg.StreamInfo.CurrentlyWatching, uint(2))
	//	require.Greater(t, msg.StreamInfo.Grade, 4.4)
	//	require.Less(t, msg.StreamInfo.Grade, 4.6)
	//	require.Equal(t, msg.Data.Chunk, chunks[ind])
	//	require.Equal(t, msg.Data.StartIndex, uint(leftInd))
	//	leftInd = leftInd + len(chunks[ind])
	//}

	log.Info().Msgf("Test Done")

}

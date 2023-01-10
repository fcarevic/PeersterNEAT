package unit

import (
	"bytes"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
	"testing"
	"time"
)

// import (
//
//	"bufio"
//	"bytes"
//	"fmt"
//	"math/rand"
//	"regexp"
//	"strings"
//	"testing"
//	"time"
//
//	"github.com/stretchr/testify/require"
//	z "go.dedis.ch/cs438/internal/testing"
//	"go.dedis.ch/cs438/peer"
//	"go.dedis.ch/cs438/transport"
//	"go.dedis.ch/cs438/transport/channel"
//
// )
//
// // 1-2
// //
// // Node starts streaming, but there is no peers wanting to join, thus the stream is finished without
// sending any packet (not counting the announcement).
// The sequence is following
// 1-2 Rumor (with StartStreamMessage)
func Test_Project_Stream_No_Clients(t *testing.T) {
	transp := channel.NewTransport()
	chunkSize := uint(64*3 + 2) // The metafile can handle just 3 chunks

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithAutostart(false))
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithAutostart(false))
	defer node1.Stop()
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.Start()
	node2.Start()

	chunk1 := make([]byte, chunkSize)
	chunk2 := make([]byte, chunkSize)
	chunk3 := make([]byte, chunkSize/3)

	chunk1[0] = 0xa
	chunk2[0] = 0xb
	chunk3[0] = 0xc

	data := append(chunk1, append(chunk2, chunk3...)...)

	buf := bytes.NewBuffer(data)

	streamID, err := node1.AnnounceStartStreaming("file1", 10, []byte{})
	require.NoError(t, err)

	// Wait for announcement and at least one packet to be sent
	time.Sleep(time.Millisecond)

	clients, err := node1.GetClients(streamID)
	require.NoError(t, err)
	require.Len(t, clients, 0)

	errStream := node1.Stream(buf, "file1", 10, streamID, []byte{}, 0)
	require.NoError(t, errStream)

	// Wait for stream to finish
	time.Sleep(50 * time.Millisecond)

	n1Ins := node1.GetIns()
	n1Outs := node1.GetOuts()

	n2Ins := node2.GetIns()
	n2Outs := node2.GetOuts()

	// Node 1 should have sent only the announcement and received ack
	require.Len(t, n1Outs, 1)
	require.Len(t, n1Ins, 1)
	require.Equal(t, "rumor", n1Outs[0].Msg.Type)

	rumors := z.GetRumor(t, n1Outs[0].Msg)
	require.Len(t, rumors.Rumors, 1)
	require.Equal(t, "streamstartmessage", rumors.Rumors[0].Msg.Type)

	require.Equal(t, "ack", n1Ins[0].Msg.Type)

	// Node 2 should have received only the announcement and sent ack
	require.Len(t, n2Ins, 1)
	require.Len(t, n2Outs, 1)
	require.Equal(t, "rumor", n2Ins[0].Msg.Type)
	rumors = z.GetRumor(t, n2Ins[0].Msg)
	require.Len(t, rumors.Rumors, 1)
	require.Equal(t, "streamstartmessage", rumors.Rumors[0].Msg.Type)
	require.Equal(t, "ack", n2Outs[0].Msg.Type)

}

// // 2-2
// //
// // Node 1 streams. Node 2 becomes a client
// Topology:  1 <-> 2
// The sequence is following:
//		1. 		1 - 2 Rumor (with StartStreamMessage)

// 		2. 		2 - 1 MulticastJoinMessage (contains StreamJoinMessage)
// 		 		1 - 2 StreamAcceptMessage
// 		3. 		2 - 1 MulticastMessage(StreamDataMessage(StreamMessage))

func Test_Project_AnnounceStartAndStream(t *testing.T) {
	transp := channel.NewTransport()
	chunkSize := uint(64*3 + 2) // The metafile can handle just 3 chunks

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(1), z.WithTotalPeers(2), z.WithAntiEntropy(time.Second))
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(2), z.WithTotalPeers(2), z.WithAntiEntropy(time.Second))

	defer node1.Stop()
	defer node2.Stop()

	node1.Start()
	node2.Start()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	chunk1 := make([]byte, chunkSize)
	chunk2 := make([]byte, chunkSize)
	chunk3 := make([]byte, chunkSize/3)

	chunks := [][]byte{chunk1, chunk2, chunk3}

	chunk1[0] = 0xa
	chunk2[0] = 0xb
	chunk3[0] = 0xc

	time.Sleep(time.Second * 2)

	data := append(chunk1, append(chunk2, chunk3...)...)

	fileName := "file"
	price := 10

	buf := bytes.NewBuffer(data)

	streamID, err := node1.AnnounceStartStreaming(fileName, uint(price), []byte{})
	require.NoError(t, err)

	// Wait for announcement to finish
	time.Sleep(10 * time.Millisecond)
	err = node2.ConnectToStream(streamID, node1.GetAddr())
	require.NoError(t, err)

	// Wait for announcement to finish
	time.Sleep(10 * time.Millisecond)

	clients, err := node1.GetClients(streamID)
	require.NoError(t, err)
	require.Len(t, clients, 1)

	// Stream
	err = node1.Stream(buf, fileName, uint(price), streamID, []byte{}, 0)
	require.NoError(t, err)

	// Wait for stream to finish
	time.Sleep(50 * time.Millisecond)

	// Node 1 should have sent:
	//    Rumor(StreamStartMessage)
	//    StreamAcceptMessage
	//    MulticastMessage(StreamData)
	//    MulticastMessage(StreamData)
	//    MulticastMessage(StreamData)

	n1Outs := node1.GetOuts()
	i := 0
	arrived := false
	for i < len(n1Outs) {
		log.Info().Msgf("%s", n1Outs[i].Msg.Type)
		if n1Outs[i].Msg.Type == "rumor" {
			rumors := z.GetRumor(t, n1Outs[i].Msg)
			if rumors.Rumors[0].Msg.Type == "streamstartmessage" {
				arrived = true
				i++
				break
			}
		}
		i++
	}
	require.Equal(t, arrived, true)

	arrived = false
	log.Info().Msgf("%d", i)
	for i < len(n1Outs) {
		log.Info().Msgf("%s", n1Outs[i].Msg.Type)
		if n1Outs[i].Msg.Type == "streamacceptmessage" {
			arrived = true
			i++
			break
		}
		i++
	}
	log.Info().Msgf("%d", i)
	require.Equal(t, true, arrived)

	cnt := 0
	for i < len(n1Outs) {
		if n1Outs[i].Msg.Type == "multicastmessage" {
			cnt++
			multicastMsg := z.GetMulticastMessage(t, n1Outs[i].Msg)
			require.Equal(t, "streamdatamessage", multicastMsg.Message.Type)
		}
		i++
	}
	require.Equal(t, cnt, 3)

	// Node 1 should have received:
	//  	MulticastJoinMessage(StreamConnectMessage)
	n1Ins := node1.GetIns()
	arrived = false
	for i := range n1Ins {
		if n1Ins[i].Msg.Type == "multicastjoinmessage" {
			require.Equal(t, "multicastjoinmessage", n1Ins[i].Msg.Type)
			multicastJoinMsg := z.GetMulticastJoinMessage(t, n1Ins[i].Msg)
			require.Equal(t, "streamconnectmessage", multicastJoinMsg.Message.Type)
			arrived = true
		}
	}
	require.Equal(t, arrived, true)

	//// Node 2 should have received:
	//  	Rumor(StreamStartMessage)
	//  	StreamAcceptMessage
	//      MulticastMessage(StreamData)
	//      MulticastMessage(StreamData)
	//      MulticastMessage(StreamData)

	i = 0
	arrived = false
	n2Ins := node2.GetIns()
	for i < len(n2Ins) {
		log.Info().Msgf("%s", n2Ins[i].Msg.Type)
		if n1Outs[i].Msg.Type == "rumor" {
			rumors := z.GetRumor(t, n2Ins[i].Msg)
			if rumors.Rumors[0].Msg.Type == "streamstartmessage" {
				arrived = true
				i++
				break
			}
		}
		i++
	}
	require.Equal(t, arrived, true)

	arrived = false
	log.Info().Msgf("%d", i)
	for i < len(n2Ins) {
		log.Info().Msgf("%s", n2Ins[i].Msg.Type)
		if n2Ins[i].Msg.Type == "streamacceptmessage" {
			arrived = true
			i++
			break
		}
		i++
	}
	log.Info().Msgf("%d", i)
	require.Equal(t, true, arrived)

	cnt = 0
	for i < len(n2Ins) {
		if n2Ins[i].Msg.Type == "multicastmessage" {
			cnt++
			multicastMsg := z.GetMulticastMessage(t, n2Ins[i].Msg)
			require.Equal(t, "streamdatamessage", multicastMsg.Message.Type)
		}
		i++
	}
	require.Equal(t, cnt, 3)

	streamMsgs, errC := node2.GetNextChunks(streamID, 3)
	require.NoError(t, errC)
	leftInd := 0
	for ind, msg := range streamMsgs {
		require.Equal(t, msg.StreamInfo.Name, fileName)
		require.Equal(t, msg.StreamInfo.Price, uint(price))
		require.Equal(t, msg.StreamInfo.CurrentlyWatching, uint(1))
		require.Equal(t, msg.StreamInfo.Grade, 0.0)
		require.Equal(t, msg.Data.Chunk, chunks[ind])
		require.Equal(t, msg.Data.StartIndex, uint(leftInd))
		leftInd = leftInd + len(chunks[ind])
	}

	//// Node 2 should have sent:
	////  	MulticastJoinMessage(StreamConnectMessage)
	n2Outs := node2.GetOuts()
	arrived = false
	for i := range n2Outs {
		if n2Outs[i].Msg.Type == "multicastjoinmessage" {
			require.Equal(t, "multicastjoinmessage", n2Outs[i].Msg.Type)
			multicastJoinMsg := z.GetMulticastJoinMessage(t, n2Outs[i].Msg)
			require.Equal(t, "streamconnectmessage", multicastJoinMsg.Message.Type)
			arrived = true
		}
	}
	require.Equal(t, arrived, true)
	log.Info().Msgf("Test Done")
}

func Test_Project_SimpleStream(t *testing.T) {
	transp := channel.NewTransport()
	chunkSize := uint(64*3 + 2) // The metafile can handle just 3 chunks

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(1), z.WithTotalPeers(2), z.WithAntiEntropy(time.Second))
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(2), z.WithTotalPeers(2), z.WithAntiEntropy(time.Second))
	defer node1.Stop()
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node1.Start()
	node2.Start()

	chunk1 := make([]byte, chunkSize)
	chunk2 := make([]byte, chunkSize)
	chunk3 := make([]byte, chunkSize/3)

	chunks := [][]byte{chunk1, chunk2, chunk3}

	chunk1[0] = 0xa
	chunk2[0] = 0xb
	chunk3[0] = 0xc

	time.Sleep(time.Second * 2)

	data := append(chunk1, append(chunk2, chunk3...)...)

	fileName := "file"
	price := 10

	buf := bytes.NewBuffer(data)

	streamID, err := node1.AnnounceStartStreaming(fileName, uint(price), []byte{})
	require.NoError(t, err)

	// Wait for announcement to finish
	time.Sleep(10 * time.Millisecond)
	err = node2.ConnectToStream(streamID, node1.GetAddr())
	require.NoError(t, err)

	// Wait for announcement to finish
	time.Sleep(10 * time.Millisecond)

	clients, err := node1.GetClients(streamID)
	require.NoError(t, err)
	require.Len(t, clients, 1)

	// Stream
	err = node1.Stream(buf, fileName, uint(price), streamID, []byte{}, 0)
	require.NoError(t, err)

	// Wait for stream to finish
	time.Sleep(50 * time.Millisecond)

	// Node 1 should have sent:
	//    Rumor(StreamStartMessage)
	// 	  StreamAcceptMessage
	//    MulticastMessage(StreamData)
	//    MulticastMessage(StreamData)
	//    MulticastMessage(StreamData)

	n1Outs := node1.GetOuts()
	i := 0
	arrived := false
	for i < len(n1Outs) {
		log.Info().Msgf("%s", n1Outs[i].Msg.Type)
		if n1Outs[i].Msg.Type == "rumor" {
			rumors := z.GetRumor(t, n1Outs[i].Msg)
			if rumors.Rumors[0].Msg.Type == "streamstartmessage" {
				arrived = true
				i++
				break
			}
		}
		i++
	}
	require.Equal(t, arrived, true)

	arrived = false
	log.Info().Msgf("%d", i)
	for i < len(n1Outs) {
		log.Info().Msgf("%s", n1Outs[i].Msg.Type)
		if n1Outs[i].Msg.Type == "streamacceptmessage" {
			arrived = true
			i++
			break
		}
		i++
	}
	log.Info().Msgf("%d", i)
	require.Equal(t, true, arrived)

	cnt := 0
	for i < len(n1Outs) {
		if n1Outs[i].Msg.Type == "multicastmessage" {
			cnt++
			multicastMsg := z.GetMulticastMessage(t, n1Outs[i].Msg)
			require.Equal(t, "streamdatamessage", multicastMsg.Message.Type)
		}
		i++
	}
	require.Equal(t, cnt, 3)

	// Node 1 should have received:
	// 		MulticastJoinMessage(StreamJoinMessage)
	n1Ins := node1.GetIns()
	arrived = false
	for i := range n1Ins {
		if n1Ins[i].Msg.Type == "multicastjoinmessage" {
			require.Equal(t, "multicastjoinmessage", n1Ins[i].Msg.Type)
			multicastJoinMsg := z.GetMulticastJoinMessage(t, n1Ins[i].Msg)
			require.Equal(t, "streamconnectmessage", multicastJoinMsg.Message.Type)
			arrived = true
		}
	}
	require.Equal(t, arrived, true)

	//// Node 2 should have received:
	//  	Rumor(StreamStartMessage)
	//  	StreamAcceptMessage
	//    MulticastMessage(StreamData)
	//    MulticastMessage(StreamData)
	//    MulticastMessage(StreamData)

	i = 0
	arrived = false
	n2Ins := node2.GetIns()
	for i < len(n2Ins) {
		log.Info().Msgf("%s", n2Ins[i].Msg.Type)
		if n2Ins[i].Msg.Type == "rumor" {
			rumors := z.GetRumor(t, n2Ins[i].Msg)
			if rumors.Rumors[0].Msg.Type == "streamstartmessage" {
				arrived = true
				i++
				break
			}
		}
		i++
	}
	require.Equal(t, arrived, true)

	arrived = false
	log.Info().Msgf("%d", i)
	for i < len(n2Ins) {
		log.Info().Msgf("%s", n2Ins[i].Msg.Type)
		if n2Ins[i].Msg.Type == "streamacceptmessage" {
			arrived = true
			i++
			break
		}
		i++
	}
	log.Info().Msgf("%d", i)
	require.Equal(t, true, arrived)

	cnt = 0
	for i < len(n2Ins) {
		if n2Ins[i].Msg.Type == "multicastmessage" {
			cnt++
			multicastMsg := z.GetMulticastMessage(t, n2Ins[i].Msg)
			require.Equal(t, "streamdatamessage", multicastMsg.Message.Type)
		}
		i++
	}
	require.Equal(t, cnt, 3)

	streamMsgs, errC := node2.GetNextChunks(streamID, 3)
	require.NoError(t, errC)
	leftInd := 0
	for ind, msg := range streamMsgs {
		require.Equal(t, msg.StreamInfo.Name, fileName)
		require.Equal(t, msg.StreamInfo.Price, uint(price))
		require.Equal(t, msg.StreamInfo.CurrentlyWatching, uint(1))
		require.Equal(t, msg.StreamInfo.Grade, 0.0)
		require.Equal(t, msg.Data.Chunk, chunks[ind])
		require.Equal(t, msg.Data.StartIndex, uint(leftInd))
		leftInd = leftInd + len(chunks[ind])
	}

	// Node 2 should have sent:
	//  	MulticastJoinMessage(StreamJoinMessage)
	n2Outs := node2.GetOuts()
	arrived = false
	for i := range n2Outs {
		if n2Outs[i].Msg.Type == "multicastjoinmessage" {
			require.Equal(t, "multicastjoinmessage", n2Outs[i].Msg.Type)
			multicastJoinMsg := z.GetMulticastJoinMessage(t, n2Outs[i].Msg)
			require.Equal(t, "streamconnectmessage", multicastJoinMsg.Message.Type)
			arrived = true
		}
	}
	require.Equal(t, arrived, true)

	// Announce end of streaming
	err = node1.AnnounceStopStreaming(streamID)
	require.NoError(t, err)

	// Error should be thrown on a stream that does not exist
	clients, err = node1.GetClients(streamID)
	require.Error(t, err)
	log.Info().Msgf("Test Done")
}

//
//func Test_Project_FFMPG4_Stream(t *testing.T) {
//	transp := udp.NewUDP()
//	chunkSize := uint(12 * 1024) // The metafile can handle just 3 chunks
//
//	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(1), z.WithTotalPeers(2), z.WithAntiEntropy(time.Second))
//	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(1), z.WithTotalPeers(2), z.WithAntiEntropy(time.Second))
//	defer node1.Stop()
//	defer node2.Stop()
//
//	node1.AddPeer(node2.GetAddr())
//	node2.AddPeer(node1.GetAddr())
//	node1.Start()
//	node2.Start()
//
//	movieName := "file"
//	price := 10
//
//	streamID, err := node1.AnnounceStartStreaming(movieName, uint(price), []byte{})
//	require.NoError(t, err)
//
//	// Wait for announcement to finish
//	time.Sleep(10 * time.Millisecond)
//	err = node2.ConnectToStream(streamID, node1.GetAddr())
//	require.NoError(t, err)
//
//	// Wait for announcement to finish
//	time.Sleep(10 * time.Millisecond)
//
//	clients, err := node1.GetClients(streamID)
//	require.NoError(t, err)
//	require.Len(t, clients, 1)
//
//	// Stream
//	manifestName := "cetvhgn13hoh536ron4g.m3u8"
//	dir := "/mnt/c/Users/work/Desktop/EPFL/semester3/decentr/homeworks/video"
//	go node1.StreamFFMPG4(manifestName, dir, movieName, uint(price), streamID, []byte{})
//
//	dir = "/mnt/c/Users/work/Desktop/EPFL/semester3/decentr/homeworks/recvvideo"
//	err = node2.ReceiveFFMPG4(streamID, dir)
//	time.Sleep(120 * time.Second)
//	// Node2 Should HAVE received all chunks
//	require.NoError(t, err)
//	log.Info().Msgf("Test Done")
//}

// // 3-2
// //
// // Node 1 streams. Node 3 becomes a client
// Topology:  1 <-> 2 <-> 3
// The sequence is following:
//		Node 1 announces streaming
//		1. 		1 - 2 Rumor (with StartStreamMessage)

//		2.		2 - 3 Rumor (with StartStreamMessage)

// 		Node 3 wants to join the stream
// 		3. 		3 - 2 MulticastJoinMessage (contains StreamJoinMessage)
// 		 		2 - 1 MulticastJoinMessage (contains StreamJoinMessage)
//				1 - 3 StreamAcceptMessage

//		Node 1 streams:
// 		4. 		1 - 2 MulticastMessage(StreamDataMessage(StreamMessage))
// 		 		2 - 3 MulticastMessage(StreamDataMessage(StreamMessage))
// 		And so on, depending on the number of chunks streamed...

func Test_Project_RelayedStream(t *testing.T) {
	transp := channel.NewTransport()
	chunkSize := uint(64*3 + 2) // The metafile can handle just 3 chunks

	node1 := z.NewTestNode(
		t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize),
		z.WithContinueMongering(0), z.WithAutostart(false), z.WithPaxosID(1), z.WithTotalPeers(2),
		z.WithAntiEntropy(time.Second),
	)
	node2 := z.NewTestNode(
		t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize),
		z.WithContinueMongering(0), z.WithAutostart(false), z.WithPaxosID(2), z.WithTotalPeers(2),
		z.WithAntiEntropy(time.Second),
	)
	node3 := z.NewTestNode(
		t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize),
		z.WithContinueMongering(0), z.WithAutostart(false), z.WithPaxosID(3), z.WithTotalPeers(2),
		z.WithAntiEntropy(time.Second),
	)
	defer node1.Stop()
	defer node2.Stop()
	defer node3.Stop()

	node1.AddPeer(node2.GetAddr())
	// node 1 see node 3 via node 2
	node1.SetRoutingEntry(node3.GetAddr(), node2.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node2.GetAddr())

	node1.Start()
	node2.Start()
	node3.Start()

	time.Sleep(time.Second * 2)

	chunk1 := make([]byte, chunkSize)
	chunk2 := make([]byte, chunkSize)
	chunk3 := make([]byte, chunkSize/3)
	chunks := [][]byte{chunk1, chunk2, chunk3}
	chunk1[0] = 0xa
	chunk2[0] = 0xb
	chunk3[0] = 0xc

	data := append(chunk1, append(chunk2, chunk3...)...)

	fileName := "file"
	price := 10

	buf := bytes.NewBuffer(data)

	streamID, err := node1.AnnounceStartStreaming(fileName, uint(price), []byte{})
	require.NoError(t, err)

	// Wait for announcement to finish
	time.Sleep(10 * time.Millisecond)
	err = node3.ConnectToStream(streamID, node1.GetAddr())
	require.NoError(t, err)

	// Wait for announcement to finish
	time.Sleep(10 * time.Millisecond)

	clients, err := node1.GetClients(streamID)
	require.NoError(t, err)
	require.Len(t, clients, 1)

	// Stream
	err = node1.Stream(buf, fileName, uint(price), streamID, []byte{}, 0)
	require.NoError(t, err)

	// Wait for stream to finish
	time.Sleep(500 * time.Millisecond)

	// Node 1 should have sent:
	//  	Rumor(StreamStartMessage)
	//  	StreamAcceptMessage
	//    MulticastMessage(StreamData)
	//    MulticastMessage(StreamData)
	//    MulticastMessage(StreamData)

	n1Outs := node1.GetOuts()
	i := 0
	arrived := false
	for i < len(n1Outs) {
		log.Info().Msgf("%s", n1Outs[i].Msg.Type)
		if n1Outs[i].Msg.Type == "rumor" {
			rumors := z.GetRumor(t, n1Outs[i].Msg)
			if rumors.Rumors[0].Msg.Type == "streamstartmessage" {
				arrived = true
				i++
				break
			}
		}
		i++
	}
	require.Equal(t, arrived, true)

	arrived = false
	log.Info().Msgf("%d", i)
	for i < len(n1Outs) {
		log.Info().Msgf("%s", n1Outs[i].Msg.Type)
		if n1Outs[i].Msg.Type == "streamacceptmessage" {
			arrived = true
			i++
			break
		}
		i++
	}
	log.Info().Msgf("%d", i)
	require.Equal(t, true, arrived)

	cnt := 0
	for i < len(n1Outs) {
		if n1Outs[i].Msg.Type == "multicastmessage" {
			cnt++
			multicastMsg := z.GetMulticastMessage(t, n1Outs[i].Msg)
			require.Equal(t, "streamdatamessage", multicastMsg.Message.Type)
		}
		i++
	}
	require.Equal(t, cnt, 3)

	// Node 1 should have received:
	// 1.	Ack
	// 2. 	MulticastJoinMessage(StreamJoinMessage)
	n1Ins := node1.GetIns()
	arrived = false
	for i := range n1Ins {
		if n1Ins[i].Msg.Type == "multicastjoinmessage" {
			require.Equal(t, "multicastjoinmessage", n1Ins[i].Msg.Type)
			multicastJoinMsg := z.GetMulticastJoinMessage(t, n1Ins[i].Msg)
			require.Equal(t, "streamconnectmessage", multicastJoinMsg.Message.Type)
			arrived = true
		}
	}
	require.Equal(t, arrived, true)

	// Node 2 should not have any stream messages
	_, errNode2 := node2.GetNextChunks(streamID, len(chunks))
	require.Error(t, errNode2)

	//// Node 3 should have received:
	//  	Rumor(StreamStartMessage)
	//  	StreamAcceptMessage
	//    MulticastMessage(StreamData)
	//    MulticastMessage(StreamData)
	//    MulticastMessage(StreamData)

	i = 0
	arrived = false
	n3Ins := node3.GetIns()
	for i < len(n3Ins) {
		log.Info().Msgf("%s", n3Ins[i].Msg.Type)
		if n3Ins[i].Msg.Type == "rumor" {
			rumors := z.GetRumor(t, n3Ins[i].Msg)
			if rumors.Rumors[0].Msg.Type == "streamstartmessage" {
				arrived = true
				i++
				break
			}
		}
		i++
	}
	require.Equal(t, arrived, true)

	arrived = false
	log.Info().Msgf("%d", i)
	for i < len(n3Ins) {
		log.Info().Msgf("%s", n3Ins[i].Msg.Type)
		if n3Ins[i].Msg.Type == "streamacceptmessage" {
			arrived = true
			i++
			break
		}
		i++
	}
	log.Info().Msgf("%d", i)
	require.Equal(t, true, arrived)

	cnt = 0
	for i < len(n3Ins) {
		if n3Ins[i].Msg.Type == "multicastmessage" {
			cnt++
			multicastMsg := z.GetMulticastMessage(t, n3Ins[i].Msg)
			require.Equal(t, "streamdatamessage", multicastMsg.Message.Type)
		}
		i++
	}
	require.Equal(t, cnt, 3)

	streamMsgs, errC := node3.GetNextChunks(streamID, 3)
	require.NoError(t, errC)
	leftInd := 0
	for ind, msg := range streamMsgs {
		require.Equal(t, msg.StreamInfo.Name, fileName)
		require.Equal(t, msg.StreamInfo.Price, uint(price))
		require.Equal(t, msg.StreamInfo.CurrentlyWatching, uint(1))
		require.Equal(t, msg.StreamInfo.Grade, 0.0)
		require.Equal(t, msg.Data.Chunk, chunks[ind])
		require.Equal(t, msg.Data.StartIndex, uint(leftInd))
		leftInd = leftInd + len(chunks[ind])
	}

	// Node 3 should have sent:
	//  	MulticastJoinMessage(StreamJoinMessage)
	n3Outs := node3.GetOuts()
	arrived = false
	for i := range n3Outs {
		if n3Outs[i].Msg.Type == "multicastjoinmessage" {
			require.Equal(t, "multicastjoinmessage", n3Outs[i].Msg.Type)
			multicastJoinMsg := z.GetMulticastJoinMessage(t, n3Outs[i].Msg)
			require.Equal(t, "streamconnectmessage", multicastJoinMsg.Message.Type)
			arrived = true
		}
	}
	require.Equal(t, arrived, true)
	log.Info().Msgf("Test Done")

}

// // 3-2
// //
// // Node 1 streams. Node 3 becomes a client
// Topology:  1 <-> 2 <-> 3
//					2 <-> 4
// The sequence is following:
//		1. Node 1 announces streaming
//		2. Node 3 and 4 connect to stream
//		3. Node 1 streams

func Test_Project_RelayedStream_MultipleClients(t *testing.T) {
	transp := channel.NewTransport()
	chunkSize := uint(64*3 + 2) // The metafile can handle just 3 chunks

	node1 := z.NewTestNode(
		t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(1), z.WithTotalPeers(2),
		z.WithAntiEntropy(time.Second),
		z.WithContinueMongering(1), z.WithAutostart(false),
	)
	node2 := z.NewTestNode(
		t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(2), z.WithTotalPeers(2),
		z.WithAntiEntropy(time.Second),
		z.WithContinueMongering(1), z.WithAutostart(false),
	)
	node3 := z.NewTestNode(
		t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(3), z.WithTotalPeers(2),
		z.WithAntiEntropy(time.Second),
		z.WithContinueMongering(1), z.WithAutostart(false),
	)
	node4 := z.NewTestNode(
		t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(4),
		z.WithTotalPeers(2), z.WithAntiEntropy(time.Second),
		z.WithContinueMongering(1), z.WithAutostart(false),
	)
	defer node1.Stop()
	defer node2.Stop()
	defer node3.Stop()
	defer node4.Stop()

	node1.AddPeer(node2.GetAddr())
	// node 1 see nodes 3 and 4 via node 2
	node1.SetRoutingEntry(node3.GetAddr(), node2.GetAddr())
	node1.SetRoutingEntry(node4.GetAddr(), node2.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node2.GetAddr())
	node2.AddPeer(node4.GetAddr())
	node4.AddPeer(node2.GetAddr())
	// nodes 3 and 4 see nodes see node 1 via node 2
	node3.SetRoutingEntry(node1.GetAddr(), node2.GetAddr())
	node4.SetRoutingEntry(node1.GetAddr(), node2.GetAddr())

	node1.Start()
	node2.Start()
	node3.Start()
	node4.Start()

	time.Sleep(time.Second * 2)

	chunk1 := make([]byte, chunkSize)
	chunk2 := make([]byte, chunkSize)
	chunk3 := make([]byte, chunkSize/3)
	chunks := [][]byte{chunk1, chunk2, chunk3}
	chunk1[0] = 0xa
	chunk2[0] = 0xb
	chunk3[0] = 0xc

	data := append(chunk1, append(chunk2, chunk3...)...)

	fileName := "file"
	price := 10

	buf := bytes.NewBuffer(data)

	streamID, err := node1.AnnounceStartStreaming(fileName, uint(price), []byte{})
	require.NoError(t, err)

	// Wait for announcement to finish
	time.Sleep(10 * time.Millisecond)
	err = node3.ConnectToStream(streamID, node1.GetAddr())
	require.NoError(t, err)

	err = node4.ConnectToStream(streamID, node1.GetAddr())
	require.NoError(t, err)

	// Wait for joining to finish
	time.Sleep(20 * time.Millisecond)

	clients, err := node1.GetClients(streamID)
	require.NoError(t, err)
	require.Len(t, clients, 2)

	// Stream
	err = node1.Stream(buf, fileName, uint(price), streamID, []byte{}, 0)
	require.NoError(t, err)

	// Wait for stream to finish
	time.Sleep(500 * time.Millisecond)

	// Node 2 should not have any stream messages
	_, errNode2 := node2.GetNextChunks(streamID, len(chunks))
	require.Error(t, errNode2)

	// Node 3 should have received chunks
	streamMsgs, errC := node3.GetNextChunks(streamID, len(chunks))
	require.NoError(t, errC)
	leftInd := 0
	for ind, msg := range streamMsgs {
		require.Equal(t, msg.StreamInfo.Name, fileName)
		require.Equal(t, msg.StreamInfo.Price, uint(price))
		require.Equal(t, msg.StreamInfo.CurrentlyWatching, uint(2))
		require.Equal(t, msg.StreamInfo.Grade, 0.0)
		require.Equal(t, msg.Data.Chunk, chunks[ind])
		require.Equal(t, msg.Data.StartIndex, uint(leftInd))
		leftInd = leftInd + len(chunks[ind])
	}

	// Node 4 should have received chunks
	streamMsgs, errC = node4.GetNextChunks(streamID, len(chunks))
	require.NoError(t, errC)
	leftInd = 0
	for ind, msg := range streamMsgs {
		require.Equal(t, msg.StreamInfo.Name, fileName)
		require.Equal(t, msg.StreamInfo.Price, uint(price))
		require.Equal(t, msg.StreamInfo.CurrentlyWatching, uint(2))
		require.Equal(t, msg.StreamInfo.Grade, 0.0)
		require.Equal(t, msg.Data.Chunk, chunks[ind])
		require.Equal(t, msg.Data.StartIndex, uint(leftInd))
		leftInd = leftInd + len(chunks[ind])
	}

	log.Info().Msgf("Test Done")

}

func Test_Project_Rating_MultipleClients(t *testing.T) {
	transp := channel.NewTransport()
	chunkSize := uint(64*3 + 2) // The metafile can handle just 3 chunks

	node1 := z.NewTestNode(
		t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(1), z.WithTotalPeers(2),
		z.WithAntiEntropy(time.Second),
		z.WithContinueMongering(1), z.WithAutostart(false),
	)
	node2 := z.NewTestNode(
		t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(2), z.WithTotalPeers(2),
		z.WithAntiEntropy(time.Second),
		z.WithContinueMongering(1), z.WithAutostart(false),
	)
	node3 := z.NewTestNode(
		t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(3), z.WithTotalPeers(2),
		z.WithAntiEntropy(time.Second),
		z.WithContinueMongering(1), z.WithAutostart(false),
	)
	node4 := z.NewTestNode(
		t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(chunkSize), z.WithPaxosID(4),
		z.WithTotalPeers(2), z.WithAntiEntropy(time.Second),
		z.WithContinueMongering(1), z.WithAutostart(false),
	)
	defer node1.Stop()
	defer node2.Stop()
	defer node3.Stop()
	defer node4.Stop()

	node1.AddPeer(node2.GetAddr())
	// node 1 see nodes 3 and 4 via node 2
	node1.SetRoutingEntry(node3.GetAddr(), node2.GetAddr())
	node1.SetRoutingEntry(node4.GetAddr(), node2.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node2.GetAddr())
	node2.AddPeer(node4.GetAddr())
	node4.AddPeer(node2.GetAddr())
	// nodes 3 and 4 see nodes see node 1 via node 2
	node3.SetRoutingEntry(node1.GetAddr(), node2.GetAddr())
	node4.SetRoutingEntry(node1.GetAddr(), node2.GetAddr())

	node1.Start()
	node2.Start()
	node3.Start()
	node4.Start()

	time.Sleep(time.Second * 2)

	chunk1 := make([]byte, chunkSize)
	chunk2 := make([]byte, chunkSize)
	chunk3 := make([]byte, chunkSize/3)
	chunks := [][]byte{chunk1, chunk2, chunk3}
	chunk1[0] = 0xa
	chunk2[0] = 0xb
	chunk3[0] = 0xc

	data := append(chunk1, append(chunk2, chunk3...)...)

	fileName := "file"
	price := 10

	buf := bytes.NewBuffer(data)

	streamID, err := node1.AnnounceStartStreaming(fileName, uint(price), []byte{})
	require.NoError(t, err)

	// Wait for announcement to finish
	time.Sleep(10 * time.Millisecond)
	err = node3.ConnectToStream(streamID, node1.GetAddr())
	require.NoError(t, err)

	err = node4.ConnectToStream(streamID, node1.GetAddr())
	require.NoError(t, err)

	// Wait for joining to finish
	time.Sleep(20 * time.Millisecond)

	err = node3.ReactToStream(streamID, node1.GetAddr(), 5.0)
	require.NoError(t, err)
	err = node4.ReactToStream(streamID, node1.GetAddr(), 4.0)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	clients, err := node1.GetClients(streamID)
	require.NoError(t, err)
	require.Len(t, clients, 2)

	// Stream
	err = node1.Stream(buf, fileName, uint(price), streamID, []byte{}, 0)
	require.NoError(t, err)

	// Wait for stream to finish
	time.Sleep(500 * time.Millisecond)

	// Node 2 should not have any stream messages
	_, errNode2 := node2.GetNextChunks(streamID, len(chunks))
	require.Error(t, errNode2)

	// Node 3 should have received chunks
	streamMsgs, errC := node3.GetNextChunks(streamID, len(chunks))
	require.NoError(t, errC)
	leftInd := 0
	for ind, msg := range streamMsgs {
		require.Equal(t, msg.StreamInfo.Name, fileName)
		require.Equal(t, msg.StreamInfo.Price, uint(price))
		require.Equal(t, msg.StreamInfo.CurrentlyWatching, uint(2))
		require.Greater(t, msg.StreamInfo.Grade, 4.4)
		require.Less(t, msg.StreamInfo.Grade, 4.6)
		require.Equal(t, msg.Data.Chunk, chunks[ind])
		require.Equal(t, msg.Data.StartIndex, uint(leftInd))
		leftInd = leftInd + len(chunks[ind])
	}

	// Node 4 should have received chunks
	streamMsgs, errC = node4.GetNextChunks(streamID, len(chunks))
	require.NoError(t, errC)
	leftInd = 0
	for ind, msg := range streamMsgs {
		require.Equal(t, msg.StreamInfo.Name, fileName)
		require.Equal(t, msg.StreamInfo.Price, uint(price))
		require.Equal(t, msg.StreamInfo.CurrentlyWatching, uint(2))
		require.Greater(t, msg.StreamInfo.Grade, 4.4)
		require.Less(t, msg.StreamInfo.Grade, 4.6)
		require.Equal(t, msg.Data.Chunk, chunks[ind])
		require.Equal(t, msg.Data.StartIndex, uint(leftInd))
		leftInd = leftInd + len(chunks[ind])
	}

	log.Info().Msgf("Test Done")

}

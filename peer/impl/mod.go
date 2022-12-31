package impl

import (
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"sync"
	"time"
)

//DEFINE CONSTS

var (
	TIMEOUT = time.Millisecond * 10
)

// NewPeer creates a new peer. You can change the content and location of this
// function, but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.

	var n = node{
		isRunning:    false,
		routingTable: make(map[string]string),
		conf:         conf,
		notifyEnd:    make(chan string),
		rumorInfo: RumorInfo{
			peerSequences: make(map[string]uint),
			peerRumors:    make(map[string][]types.Rumor),
			pktToChannelMap: make(
				map[string]chan struct {
					transport.Packet
					types.AckMessage
				},
			),
		},
		dataSharing: DataSharing{
			catalog:                    make(peer.Catalog),
			dataRequestsMap:            make(map[string]chan []byte),
			receivedRequests:           make(map[string]bool),
			remoteFullyKnownMetahashes: make(map[string]chan string),
		},
		multiPaxos: MultiPaxos{
			paxos: Paxos{
				mapPaxosPrepareIDs:   make(map[uint]chan PaxosToSend),
				mapPaxosProposeIDs:   make(map[uint]chan types.PaxosAcceptMessage),
				mapPaxosAcceptIDs:    make(map[string][]types.PaxosAcceptMessage),
				notifyEndOfClockStep: make(chan types.BlockchainBlock, 5000),
				channelSuccCons:      make(chan string, 5000),
			},
			tlc: TLCInfo{
				mapStepListTLCMsg: make(map[uint][]types.TLCMessage),
			},
		},
		streamInfo: StreamInfo{
			mapClients:       make(map[string][]string),
			mapKeysListening: make(map[string][]byte),
			mapListening:     make(map[string][]types.StreamMessage),
			availableStreams: make([]types.StreamInfo, 0),
		},
		multicstInfo: MulticastInfo{
			mapMulticastClients: make(map[string][]string),
		},
		chatInfo: ChatInfo{
			sentMessages:     make([]peer.ChatMessageInfo, 0),
			receivedMessages: make([]peer.ChatMessageInfo, 0),
		},
		// PROJECT Naca
		crowdsInfo: CrowdsInfo{
			chunkMap:        NewAtomicChunkMap(),
			chunkChannelMap: NewAtomicChannelTable(),
		},
		// PROJECT Peja
		pkiInfo: PKIInfo{
			privateKey: nil,
			publicKey:  nil,
		},
	}

	// Add self-address to routing table
	n.routingTable[n.conf.Socket.GetAddress()] = n.conf.Socket.GetAddress()

	n.CrowdsInit(conf) // PROJECT Naca

	// PROJECT Peja
	if n.conf.AntiEntropyInterval != 0 {
		publicKey, privateKey, err := n.PkiInit(n.conf.Socket.GetAddress(), 100)
		if err != nil {
			log.Error().Msgf("pkiInit error", err)
			return nil
		}
		n.pkiInfo = PKIInfo{privateKey, publicKey}
	} else {
		n.conf.MessageRegistry.RegisterMessageCallback(types.ConfidentialityMessage{}, n.ProcessConfidentialityMessage)
	}

	// Project
	n.MulticastInit()
	n.StreamingInit()

	// Register callbacks
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.chatMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.RumorMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, n.ackMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, n.statusMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, n.privateMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, n.emptyMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.DataRequestMessage{}, n.dataRequestsMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.DataReplyMessage{}, n.dataReplyMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.SearchRequestMessage{}, n.searchRequestMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.SearchReplyMessage{}, n.searchReplyMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PaxosProposeMessage{}, n.paxosProposeMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PaxosPrepareMessage{}, n.paxosPrepareMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PaxosPromiseMessage{}, n.paxosPromiseMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PaxosAcceptMessage{}, n.paxosAcceptMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.TLCMessage{}, n.tlcMessageCallback)

	//TODO:Just for testing, should be deleted
	/*if n.conf.Socket.GetAddress() != "127.0.0.1:31111" {
		n.AddPeer("127.0.0.1:31111")
	}
	if n.conf.Socket.GetAddress() != "127.0.0.1:32222" {
		n.AddPeer("127.0.0.1:32222")
	}*/
	return &n
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// You probably want to keep the peer.Configuration on this struct:
	conf peer.Configuration

	// Streaming
	streamInfo StreamInfo

	// Mulitcast
	multicstInfo MulticastInfo

	// Chatting
	chatInfo ChatInfo

	// Crowds
	crowdsInfo CrowdsInfo

	// PKI
	pkiInfo PKIInfo

	// routing
	routingTable peer.RoutingTable

	// Rumor protocol
	rumorInfo RumorInfo

	// Data sharing
	dataSharing DataSharing

	// Paxos
	multiPaxos MultiPaxos

	// Status flags
	isRunning                   bool
	antiEntropyHeartbeatRunning bool

	// Channels
	notifyEnd chan string

	//Semaphores
	startStopMutex    sync.Mutex
	routingTableMutex sync.RWMutex
	activeThreads     sync.WaitGroup
}

// Start implements peer.Service
func (n *node) Start() error {
	// Safety checks
	if n == nil {
		log.Error().Msg("Node is nil")
		return errors.New("node is nil")
	}

	// acquire lock
	n.startStopMutex.Lock()
	defer n.startStopMutex.Unlock()

	// If the old value is true, then the node is already running
	if n.isRunning {
		log.Error().Msg("Attempt to start running node")
		return xerrors.Errorf("Node is already running.")
	}
	n.isRunning = true

	// Start threads
	n.activeThreads.Add(1)
	go mainLoop(n)
	return nil
}

func (n *node) getRunning() bool {
	n.startStopMutex.Lock()
	flag := n.isRunning
	n.startStopMutex.Unlock()
	return flag
}

// Stop implements peer.Service
func (n *node) Stop() error {
	// Safety checks
	if n == nil {
		log.Error().Msg("Node is nil")
		return xerrors.Errorf("Node is nil.")
	}

	log.Info().Msgf("[%s] odje", n.conf.Socket.GetAddress())

	// acquire lock
	n.startStopMutex.Lock()
	log.Info().Msgf("[%s] odje2", n.conf.Socket.GetAddress())
	if !n.isRunning {
		log.Info().Msgf("[%s] odje3", n.conf.Socket.GetAddress())
		n.startStopMutex.Unlock()
		log.Error().Msg("Attempt to stop non-running node")
		return xerrors.Errorf("Node is not running.")
	}
	n.isRunning = false
	close(n.notifyEnd)
	n.startStopMutex.Unlock()
	log.Info().Msgf("[%s] Waiting for threads to be done", n.conf.Socket.GetAddress())
	// Wait for all threads to finish
	n.activeThreads.Wait()
	log.Info().Msgf("[%s] Exited", n.conf.Socket.GetAddress())
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	// Safety checks
	if n == nil {
		log.Error().Msg("Node is nil")
		return xerrors.Errorf("Node is nil.")
	}

	// Get src address, next hop address and craft packet
	var src = n.conf.Socket.GetAddress()
	var relay = src
	var pkt = msgToPacket(src, relay, dest, msg)
	err := n.sendPkt(pkt, TIMEOUT)
	if err != nil {
		log.Error().Msgf("ERROR: in unicast function: %s", err.Error())
		return err
	}
	return nil
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	// Safety checks
	if n == nil {
		log.Error().Msg("Node is nil")
		return
	}

	// If node is peer, set next hop to be the address of the node
	for _, address := range addr {
		var myAddress = n.conf.Socket.GetAddress()
		if myAddress == address || address == "" {
			log.Error().Msg("Attempt to add invalid address as peer.")
			continue
		}
		n.SetRoutingEntry(address, address)

		// Start the antiEntropy if not running
		n.startStopMutex.Lock()
		if !(n.antiEntropyHeartbeatRunning) {
			n.antiEntropyHeartbeatRunning = true

			n.activeThreads.Add(1)
			go n.antiEntropy()

			n.activeThreads.Add(1)
			go n.heartbeat()
		}
		n.startStopMutex.Unlock()
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	// Safety checks
	if n == nil {
		log.Error().Msg("Node is nil")
		return nil
	}

	// Acquire lock
	n.routingTableMutex.Lock()
	defer n.routingTableMutex.Unlock()

	// Copy routing table
	var routingTableCopy = make(peer.RoutingTable)
	for origin, relay := range n.routingTable {
		routingTableCopy[origin] = relay
	}
	return routingTableCopy
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {

	// Safety checks
	if n == nil {
		log.Error().Msg("Node is nil")
		return
	}
	var myAddress = n.conf.Socket.GetAddress()
	if myAddress == origin || myAddress == relayAddr || origin == "" {
		log.Error().Msg("Attempt to add invalid address to routing table.")
		return
	}

	// Acquire routing table write Lock
	n.routingTableMutex.Lock()
	defer n.routingTableMutex.Unlock()

	if relayAddr == "" {
		// Delete from table
		delete(n.routingTable, origin)
	} else {
		// Write to a table
		n.routingTable[origin] = relayAddr
	}

}

func (n *node) Broadcast(msg transport.Message) error {

	// Create the rumor message
	err2 := n.sendMessageAsRumor(msg, []string{})
	if err2 != nil {
		log.Error().Msgf(
			"[%s]: Broadcast: %s",
			n.conf.Socket.GetAddress(),
			err2.Error(),
		)
	}

	// Process the rumor locally
	var header = transport.NewHeader(
		n.conf.Socket.GetAddress(),
		n.conf.Socket.GetAddress(),
		n.conf.Socket.GetAddress(),
		0,
	)

	// CAUTION! HERE WE PROCESS MESSAGE FROM FUNCTION ARGUMENT, NOT THE RumorMessage
	n.activeThreads.Add(1)
	go func() {
		defer n.activeThreads.Done()
		var pkt = transport.Packet{Header: &header, Msg: &msg}
		err := n.conf.MessageRegistry.ProcessPacket(pkt)
		if err != nil {
			log.Error().Msgf("%s: BROADCAST: Local message handling failed", n.conf.Socket.GetAddress())
		}
	}()

	return nil
}

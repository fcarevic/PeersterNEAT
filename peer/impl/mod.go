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
		rumorInfo: RumorInfo{
			sequenceCounter: 0,
			peerSequences:   make(map[string]uint),
			peerRumors:      make(map[string][]types.Rumor),
		},
	}

	// Add self-address to routing table
	n.routingTable[n.conf.Socket.GetAddress()] = n.conf.Socket.GetAddress()

	// Register callbacks
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.chatMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.RumorMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, n.ackMessageCallback)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, n.statusMessageCallback)

	return &n
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// You probably want to keep the peer.Configuration on this struct:
	conf peer.Configuration

	// routing
	routingTable peer.RoutingTable

	// Rumor protocol
	rumorInfo RumorInfo

	// Status flags
	isRunning          bool
	antiEntropyRunning bool

	//Semaphores
	startStopMutex    sync.Mutex
	routingTableMutex sync.RWMutex
	activeThreads     sync.WaitGroup
	antiEntropyWait   sync.WaitGroup
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

	n.activeThreads.Add(2)
	go mainLoop(n)

	n.antiEntropyWait.Add(1)
	go n.antiEntropy()
	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	// Safety checks
	if n == nil {
		log.Error().Msg("Node is nil")
		return xerrors.Errorf("Node is nil.")
	}

	// acquire lock
	n.startStopMutex.Lock()
	if !n.isRunning {
		log.Error().Msg("Attempt to stop non-running node")
		return xerrors.Errorf("Node is not running.")
	}
	n.isRunning = false

	// Check if antiEntropy is running
	if n.antiEntropyRunning {
		n.antiEntropyRunning = false
	} else {
		// if not running implies waiting for peer to be added to routing table
		// Awake it
		// TODO: THIS CANNOT BE DONE, AFTER WAIT DO I CHECK CONDITION AGAIN?
		n.antiEntropyWait.Done()
	}
	n.startStopMutex.Unlock()

	// Wait for all threads to finish
	n.activeThreads.Wait()
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

		n.startStopMutex.Lock()
		if !(n.antiEntropyRunning) {
			n.antiEntropyRunning = true
			n.antiEntropyWait.Done()
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
	_, err2 := n.sendMessageAsRumor(msg)
	if err2 != nil {
		return err2
	}

	// Process the rumor locally
	var header = transport.NewHeader(
		n.conf.Socket.GetAddress(),
		n.conf.Socket.GetAddress(),
		n.conf.Socket.GetAddress(),
		0)

	// CAUTION! HERE WE PROCESS MESSAGE FROM FUNCTION ARGUMENT, NOT THE RumorMessage
	var pkt = transport.Packet{Header: &header, Msg: &msg}
	err := n.conf.MessageRegistry.ProcessPacket(pkt)
	if err != nil {
		log.Error().Msgf("%s: BROADCAST: Local message handling failed", n.conf.Socket.GetAddress())
	}
	return nil
}

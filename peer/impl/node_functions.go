package impl

import (
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"golang.org/x/xerrors"
	"time"
)

// The main loop of the node.
// Function receives the packets, relays them or calls callback.
// Function called by the `start` method.
func mainLoop(n *node) {

	log.Info().Msgf("Starting node on: %s", n.conf.Socket.GetAddress())
	// While the isRunning flag is set, do the main loop
	for {
		// Check stop condition
		if !n.getRunning() {
			break
		}

		// Receive packet
		pkt, err := n.conf.Socket.Recv(TIMEOUT)

		// Check for errors
		if errors.Is(err, transport.TimeoutError(TIMEOUT)) {
			continue
		}
		if err != nil {
			log.Error().Msg(err.Error())
		}

		var myAddress = n.conf.Socket.GetAddress()
		// If message is for me, then process it else relay it
		if pkt.Header.Destination == myAddress {

			n.activeThreads.Add(1)

			go func() {
				err := n.conf.MessageRegistry.ProcessPacket(pkt)
				if err != nil {
					log.Error().Msgf("[%s]: mainLoop :Error while processsing a packet: %s:\nError: %s",
						n.conf.Socket.GetAddress(),
						pkt.String(),
						err.Error(),
					)
				}
				n.activeThreads.Done()
			}()
		} else {
			// relay the message
			err := n.sendPkt(pkt, TIMEOUT)
			if err != nil {
				log.Error().Msg(err.Error())
			}
		}
	}

	// Signal the end
	n.activeThreads.Done()
	log.Info().Msgf("Stopped node on: %s", n.conf.Socket.GetAddress())
}

// Function retrieves the address of the next hop for the destination
func (n *node) getNextHop(dst string) string {

	// Acquire read lock
	n.routingTableMutex.RLock()
	defer n.routingTableMutex.RUnlock()

	// Retrieve the next hop address
	return n.routingTable[dst]
}

// Function to send message
func (n *node) sendPkt(pkt transport.Packet, timeout time.Duration) error {

	// Get next hop address
	var nextHop = n.getNextHop(pkt.Header.Destination)

	// Send packet
	if nextHop == "" {
		return xerrors.Errorf("Unknown next_hop for %s", pkt.Header.Destination)
	}

	// Change relayed info
	pkt = pkt.Copy()
	pkt.Header.RelayedBy = n.conf.Socket.GetAddress()
	return n.conf.Socket.Send(nextHop, pkt, timeout)
}

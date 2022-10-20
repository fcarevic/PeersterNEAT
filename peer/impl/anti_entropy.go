package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/types"
	"time"
)

func (n *node) antiEntropy() {

	// If interval is 0, this should not be started
	if n.conf.AntiEntropyInterval == 0 {
		n.activeThreads.Done()
		return
	}

	// Wait for first peer to be added
	n.antiEntropyWait.Wait()
	log.Error().Msgf("[%s]: AntiEntropy started", n.conf.Socket.GetAddress())

	// AE loop
	for {

		// Check whether to stop the thread
		n.startStopMutex.Lock()
		stop := !n.isRunning
		n.startStopMutex.Unlock()
		if stop {
			break
		}

		// Create a msg
		var statusMsg, _ = n.getStatusMaps()

		// Send to neighbour
		err := n.sendToRandomNeighbour(statusMsg)
		if err != nil {
			log.Error().Msgf("[%s]: AntiEntropy: Sending failed", n.conf.Socket.GetAddress())
		}

		// Sleep
		time.Sleep(n.conf.AntiEntropyInterval)

	}
	log.Error().Msgf("[%s]: AntiEntropy stopped", n.conf.Socket.GetAddress())
	// Notify that the thread finished
	n.activeThreads.Done()
}

func (n *node) sendToRandomNeighbour(msg types.Message) error {

	// Get random neighbour
	neighbour, err := n.getRangomNeighbour([]string{})
	if err != nil {
		return err
	}

	// Get src address
	srcAddress := n.conf.Socket.GetAddress()

	// Craft a packet
	packet, err := n.msgTypesToPacket(srcAddress, srcAddress, neighbour, msg)
	if err != nil {
		log.Error().Msgf("[%s]: sendToRandomNeighbour: Marshalling failed", n.conf.Socket.GetAddress())
		return err
	}
	// Send the packet
	errSend := n.sendPkt(packet, TIMEOUT)
	if errSend != nil {
		log.Error().Msgf("[%s]: sendToRandomNeighbour: Sending message failed", n.conf.Socket.GetAddress())
		return errSend
	}
	log.Info().Msgf("[%s]: sendToRandomNeighbour: Status message sent to %s", n.conf.Socket.GetAddress(), neighbour)
	return nil
}

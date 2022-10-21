package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/types"
	"time"
)

func (n *node) heartbeat() {

	// If interval is 0, this should not be started
	if n.conf.HeartbeatInterval == 0 {
		n.activeThreads.Done()
		return
	}

	log.Error().Msgf("[%s]: Heartbeat started", n.conf.Socket.GetAddress())
	for {
		// Check whether to stop the thread
		if !n.getRunning() {
			break
		}

		// Create a msg
		var emptyMsg, err = n.conf.MessageRegistry.MarshalMessage(types.EmptyMessage{})
		if err != nil {
			log.Error().Msgf("[%s]: Heartbeat: Marshalling of empty message failed", n.conf.Socket.GetAddress())
		}

		// Broadcast
		errBroadcast := n.Broadcast(emptyMsg)
		if errBroadcast != nil {
			log.Error().Msgf("[%s]: Heartbeat: Broadcast error",
				n.conf.Socket.GetAddress(),
				errBroadcast.Error(),
			)
		}

		// Sleep
		time.Sleep(n.conf.HeartbeatInterval)

	}
	log.Error().Msgf("[%s]: Heartbeat stopped", n.conf.Socket.GetAddress())
	// Notify that the thread finished
	n.activeThreads.Done()
}

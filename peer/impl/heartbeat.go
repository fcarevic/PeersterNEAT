package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/types"
	"time"
)

func (n *node) heartbeat() {

	defer n.activeThreads.Done()

	// If interval is 0, this should not be started
	if n.conf.HeartbeatInterval == 0 {
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
			log.Error().Msgf("[%s]: Heartbeat: Broadcast error: %s",
				n.conf.Socket.GetAddress(),
				errBroadcast.Error(),
			)
		}

		// Sleep
		select {
		case <-time.After(n.conf.HeartbeatInterval):
			break
		case <-n.notifyEnd:
			break

		}

	}
	log.Error().Msgf("[%s]: Heartbeat stopped", n.conf.Socket.GetAddress())
	// Notify that the thread finished

}

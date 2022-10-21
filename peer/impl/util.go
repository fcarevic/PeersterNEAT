package impl

import (
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"sort"
)

// TTL Define const
const TTL = 0

// Util function to craft the Packet given the message
func msgToPacket(src string, relay string, dest string, msg transport.Message) transport.Packet {

	// Craft new header
	var header = transport.NewHeader(src, relay, dest, TTL)

	// Craft packet
	return transport.Packet{
		Header: &header,
		Msg:    &msg}
}

// Util function to craft the Packet given the message
func (n *node) msgTypesToPacket(src string, relay string, dest string, msg types.Message) (transport.Packet, error) {

	// Marshall message
	msgMarshalled, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		return transport.Packet{}, err
	}

	// Craft packet
	pkt := msgToPacket(src, relay, dest, msgMarshalled)
	return pkt, nil
}

// Check if string is in the slice
// Code taken from https://gosamples.dev/slice-contains/
func contains(elems []string, v string) bool {
	for _, s := range elems {
		if v == s {
			return true
		}
	}
	return false
}

func copyStatusRumorMaps(
	statusMap map[string]uint,
	rumorMap map[string][]types.Rumor) (map[string]uint,
	map[string][]types.Rumor) {
	var statusCopy = make(map[string]uint)
	for k, v := range statusMap {
		statusCopy[k] = v
	}

	var rumorMapCopy = make(map[string][]types.Rumor)

	for peer, rumorArray := range rumorMap {
		var rumorArrayCopy []types.Rumor
		rumorArrayCopy = append(rumorArrayCopy, rumorArray...)
		rumorMapCopy[peer] = rumorArrayCopy
	}

	return statusCopy, rumorMapCopy
}

// Sort rumors
func sortRumors(rumors []types.Rumor) []types.Rumor {
	sort.Slice(rumors, func(i, j int) bool {
		return rumors[i].Sequence < rumors[j].Sequence
	})
	return rumors
}

package udp

import (
	"go.dedis.ch/cs438/transport"
	"sync"
)

// PacketBuffer
// Definition of PacketBuffer
type PacketBuffer struct {
	sync.Mutex
	buffer []transport.Packet
}

// Insert new element to buffer
func (p *PacketBuffer) add(pkt transport.Packet) {

	// Acquire lock
	p.Lock()
	defer p.Unlock()

	// Add new packet
	p.buffer = append(p.buffer, pkt.Copy())
}

// Get all the elements from buffer
func (p *PacketBuffer) getAll() []transport.Packet {

	// Acquire lock
	p.Lock()
	defer p.Unlock()

	// Get all packets
	res := make([]transport.Packet, len(p.buffer))
	for i, pkt := range p.buffer {
		res[i] = pkt.Copy()
	}
	return res
}

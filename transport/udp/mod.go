package udp

import (
	"errors"
	"github.com/rs/zerolog/log"
	"net"
	"os"
	"sync"
	"time"

	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000
const NETWORK = "udp"

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {

	// Resolve UDP address
	udpAddr, err := net.ResolveUDPAddr(NETWORK, address)
	if err != nil {
		log.Error().Msgf("Error while resolving UDP address: %s", address)
		return nil, err
	}

	// Create connection
	udp, err := net.ListenUDP(NETWORK, udpAddr)
	if err != nil {
		return nil, err
	}

	var socket = Socket{
		udpConnection: udp,
		ipAddress:     udp.LocalAddr().String(),
	}

	log.Info().Msgf("Resolved address while creating socket: %s", socket.GetAddress())
	return &socket, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {

	// Ip address and udp connection
	udpConnection *net.UDPConn
	ipAddress     string

	// Buffers
	inputBuffer  PacketBuffer
	outputBuffer PacketBuffer

	// Semaphores
	connectionMutex sync.Mutex
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	return s.udpConnection.Close()
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {

	// Unmarshall packet
	bytes, err := pkt.Marshal()
	if err != nil {
		return err
	}

	// Rresolve UDP address
	udpDestAddr, err := net.ResolveUDPAddr(NETWORK, dest)
	if err != nil {
		return err
	}
	// Acquire connection lock
	s.connectionMutex.Lock()
	defer s.connectionMutex.Unlock()

	// Set timeout
	var totalTime = time.Time{}
	if timeout != 0 {
		totalTime = time.Now().Add(timeout)
	}
	errt := s.udpConnection.SetWriteDeadline(totalTime)
	if errt != nil {
		return err
	}

	// Send packet
	_, err = s.udpConnection.WriteToUDP(bytes, udpDestAddr)
	if err != nil {
		log.Error().Msgf("%v\n", err)
		return err
	}

	// Add to the buffer of sent packets
	s.outputBuffer.add(pkt)
	//log.Info().Msgf("%s: SOCKET SEND: successfully sent message: %s", s.GetAddress(), pkt)
	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {

	// Acquire lock
	s.connectionMutex.Lock()
	defer s.connectionMutex.Unlock()

	// Set timeout
	var totalTime = time.Time{}
	if timeout != 0 {
		totalTime = time.Now().Add(timeout)
	}
	var err = s.udpConnection.SetReadDeadline(totalTime)
	if err != nil {
		return transport.Packet{}, err
	}
	// Read bytes from socket
	var buffer = make([]byte, bufSize)
	var bytesReceived, _, errn = s.udpConnection.ReadFromUDP(buffer)

	if errn != nil {
		// check if timeout error
		var successfulCast = errors.Is(errn, os.ErrDeadlineExceeded)
		if successfulCast {
			return transport.Packet{}, transport.TimeoutError(timeout)
		}
		log.Error().Msgf("%s: SOCKET RECEIVE: error \t %s", s.GetAddress(), errn.Error())
		return transport.Packet{}, errn

	}

	// Unmarshall packet
	var pkt = transport.Packet{}
	unmarshallErr := pkt.Unmarshal(buffer[:bytesReceived])
	if unmarshallErr != nil {
		log.Error().Msgf("%s: SOCKET RECEIVE: Unmarsalling failed", s.GetAddress())
		return pkt, err
	}

	//WRITE TO A BUFFER
	s.inputBuffer.add(pkt)
	//log.Info().Msgf("%s: SOCKET RECEIVE: successfully received packet: %s", s.GetAddress(), pkt.String())
	return pkt, nil

}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.ipAddress
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	return s.inputBuffer.getAll()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	return s.outputBuffer.getAll()
}

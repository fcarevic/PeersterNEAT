package peer

import (
	"crypto/rsa"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// Crowds defines the functions for crowds messaging
type PKI interface {
	PutInitialBlockOnChain(*rsa.PublicKey, string, float64) error
	PkiInit(string, float64) (*rsa.PublicKey, *rsa.PrivateKey, error)
	SendEncryptedMsg(transport.Message, *rsa.PublicKey) ([]byte, error)
	DecryptedMsg([]byte, *rsa.PrivateKey) ([]byte, error)
	ProcessConfidentialityMessage(types.Message, transport.Packet) error
	GetPublicKey(string) (*rsa.PublicKey, error)
	GetAmount(string) (float64, error)
	PaySubscription(string, string, string, float64) error
	PaySubscriptionFull(string, *rsa.PublicKey, string, *rsa.PublicKey, string, float64) error
	IsPayedSubscription(string, string, string, float64) (bool, error)
	SetInitBlock(*rsa.PublicKey, string, float64) error
}

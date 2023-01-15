package peer

import (
	"crypto/rsa"
	"go.dedis.ch/cs438/transport"
)

// Crowds defines the functions for crowds messaging
type PKI interface {
	SendEncryptedMsg(transport.Message, string) ([]byte, error)
	DecryptedMsg([]byte, *rsa.PrivateKey) ([]byte, error)
	GetPublicKey(string) (*rsa.PublicKey, error)
	GetAmount(string) (float64, error)
	PaySubscription(string, string, string, float64) error
	PaySubscriptionFull(string, *rsa.PublicKey, string, *rsa.PublicKey, string, float64) error
	IsPayedSubscription(string, string, string, float64) (bool, error)
	PutInitialBlockOnChain(*rsa.PublicKey, string, float64) error

	//PkiInit(string, float64) (*rsa.PublicKey, *rsa.PrivateKey, error)
	//ProcessConfidentialityMessage(types.Message, transport.Packet) error
	//SetInitBlock(*rsa.PublicKey, string, float64) error
}

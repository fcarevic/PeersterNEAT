package controller

import (
	"crypto/rsa"
	"encoding/json"
	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"io"
	"net/http"
)

// NewPKI returns a new initialized pki.
func NewPKI(node peer.Peer, log *zerolog.Logger) pki {
	return pki{
		node: node,
		log:  log,
	}
}

type pki struct {
	node peer.Peer
	log  *zerolog.Logger
}

type PKISendBody struct {
	msg       transport.Message
	publicKey *rsa.PublicKey
}

func (c pki) PKISend() http.HandlerFunc {
	// pozvati ficinu fju za registrovanje poruke da andrija ne pamti na frontu - opciono

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKISendBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(w, "failed to unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError)
				return
			}

			msg, err := c.node.SendEncryptedMsg(res.msg, res.publicKey)
			if err != nil {
				return
			}
			_, err = w.Write(msg)
			if err != nil {
				return
			}

		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

type PKIDecryptBody struct {
	cipherMsg  []byte
	privateKey *rsa.PrivateKey
}

func (c pki) PKIDecrypt() http.HandlerFunc {
	// pozvati ficinu fju za registrovanje poruke da andrija ne pamti na frontu - opciono

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKIDecryptBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(w, "failed to unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError)
				return
			}

			msg, err := c.node.DecryptedMsg(res.cipherMsg, res.privateKey)
			if err != nil {
				return
			}
			_, err = w.Write(msg)
			if err != nil {
				return
			}

		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

type PKIPublicKeyBody struct {
	address string
}

func (c pki) PKIPublicKey() http.HandlerFunc {
	// pozvati ficinu fju za registrovanje poruke da andrija ne pamti na frontu - opciono

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKIPublicKeyBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(w, "failed to unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError)
				return
			}

			key, err := c.node.GetPublicKey(res.address)
			if err != nil {
				return
			}
			marshal, err := json.Marshal(key)
			if err != nil {
				return
			}
			_, err = w.Write(marshal)
			if err != nil {
				return
			}

		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

type PKIAmountBody struct {
	address string
}

func (c pki) PKIAmount() http.HandlerFunc {
	// pozvati ficinu fju za registrovanje poruke da andrija ne pamti na frontu - opciono

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKIAmountBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(w, "failed to unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError)
				return
			}

			amount, err := c.node.GetAmount(res.address)
			if err != nil {
				return
			}
			marshal, err := json.Marshal(amount)
			if err != nil {
				return
			}
			_, err = w.Write(marshal)
			if err != nil {
				return
			}

		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

type PKIPaySubscriptionBody struct {
	senderAddress, receiverAddress, streamID string
	amount                                   float64
}

func (c pki) PKIPaySubscription() http.HandlerFunc {
	// pozvati ficinu fju za registrovanje poruke da andrija ne pamti na frontu - opciono

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKIPaySubscriptionBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(w, "failed to unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError)
				return
			}

			err = c.node.PaySubscription(res.senderAddress, res.receiverAddress, res.streamID, res.amount)
			if err != nil {
				return
			}

		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

type PKIPaySubscriptionFullBody struct {
	senderAddress     string
	senderPublicKey   *rsa.PublicKey
	receiverAddress   string
	receiverPublicKey *rsa.PublicKey
	streamID          string
	amount            float64
}

func (c pki) PKIPaySubscriptionFull() http.HandlerFunc {
	// pozvati ficinu fju za registrovanje poruke da andrija ne pamti na frontu - opciono

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKIPaySubscriptionFullBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(w, "failed to unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError)
				return
			}

			err = c.node.PaySubscriptionFull(res.senderAddress, res.senderPublicKey,
				res.receiverAddress, res.receiverPublicKey, res.streamID, res.amount)
			if err != nil {
				return
			}

		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

type PKICheckPaidBody struct {
	senderAddress, receiverAddress, streamID string
	amount                                   float64
}

func (c pki) PKICheckPaid() http.HandlerFunc {
	// pozvati ficinu fju za registrovanje poruke da andrija ne pamti na frontu - opciono

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKICheckPaidBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(w, "failed to unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError)
				return
			}

			subscription, err := c.node.IsPayedSubscription(res.senderAddress, res.receiverAddress, res.streamID, res.amount)
			if err != nil {
				return
			}
			marshal, err := json.Marshal(subscription)
			if err != nil {
				return
			}
			_, err = w.Write(marshal)
			if err != nil {
				return
			}

		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

type PKIPutInitialBlockOnChainBody struct {
	publicKey *rsa.PublicKey
	address   string
	amount    float64
}

func (c pki) PKIPutInitialBlockOnChain() http.HandlerFunc {
	// pozvati ficinu fju za registrovanje poruke da andrija ne pamti na frontu - opciono

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKIPutInitialBlockOnChainBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(w, "failed to unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError)
				return
			}

			err = c.node.PutInitialBlockOnChain(res.publicKey, res.address, res.amount)
			if err != nil {
				return
			}

		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

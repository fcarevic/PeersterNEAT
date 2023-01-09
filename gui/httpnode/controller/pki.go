package controller

import (
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
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
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKISendBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}
			fmt.Println("asd")
			msg, err := c.node.SendEncryptedMsg(res.msg, res.publicKey)
			if err != nil {
				return
			}
			_, err = w.Write(msg)
			if err != nil {
				return
			}
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
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
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKIDecryptBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
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
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
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
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKIPublicKeyBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
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
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (c pki) PKIAmount() http.HandlerFunc {
	// pozvati ficinu fju za registrovanje poruke da andrija ne pamti na frontu - opciono

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			w.Header().Set("Content-Type", "application/json")
			address := r.URL.Query().Get("address")

			amount, err := c.node.GetAmount(address)
			fmt.Println(amount)
			fmt.Println(err)
			if err != nil {
				http.Error(w, "error getting amount", http.StatusInternalServerError)
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
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
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
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKIPaySubscriptionBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}

			err = c.node.PaySubscription(res.senderAddress, res.receiverAddress, res.streamID, res.amount)
			if err != nil {
				return
			}
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
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
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKIPaySubscriptionFullBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}

			err = c.node.PaySubscriptionFull(
				res.senderAddress, res.senderPublicKey,
				res.receiverAddress, res.receiverPublicKey, res.streamID, res.amount,
			)
			if err != nil {
				return
			}
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
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
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKICheckPaidBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}

			subscription, err := c.node.IsPayedSubscription(
				res.senderAddress,
				res.receiverAddress,
				res.streamID,
				res.amount,
			)
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
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
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
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := PKIPutInitialBlockOnChainBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}

			err = c.node.PutInitialBlockOnChain(res.publicKey, res.address, res.amount)
			if err != nil {
				return
			}
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

type SendPrivateMessageBody struct {
	Body string `json:"Body"`
	To   string `json:"To"`
}

func (p pki) SendPrivateMessage() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}
			fmt.Println(string(buf))
			res := SendPrivateMessageBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}

			fmt.Println("****")
			fmt.Println(res)
			fmt.Println(res.To)
			fmt.Println("***")
			key, err := p.node.GetPublicKey(res.To)
			if err != nil {
				http.Error(
					w, "failed To get public key: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}
			msg := types.ChatMessage{Message: res.Body}
			payload, err := json.Marshal(msg)
			if err != nil {
				http.Error(
					w, "failed To marshall message: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}
			transportMsg := transport.Message{
				Type:    msg.Name(),
				Payload: payload,
			}
			_, err = p.node.SendEncryptedMsg(transportMsg, key)
			if err != nil {
				http.Error(
					w, "failed To send private message: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
			return
		}
	}
}

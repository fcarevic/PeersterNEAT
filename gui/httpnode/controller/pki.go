package controller

import (
	"crypto/rsa"
	"encoding/json"
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

type PKIDecryptBody struct {
	CipherMsg  []byte          `json:"cipherMsg"`
	PrivateKey *rsa.PrivateKey `json:"privateKey"`
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

			msg, err := c.node.DecryptedMsg(res.CipherMsg, res.PrivateKey)
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
	Address string `json:"address"`
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

			key, err := c.node.GetPublicKey(res.Address)
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
			if err != nil {
				http.Error(w, "error getting Amount", http.StatusInternalServerError)
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
	SenderAddress   string  `json:"senderAddress"`
	ReceiverAddress string  `json:"receiverAddress"`
	StreamID        string  `json:"streamID"`
	Amount          float64 `json:"amount"`
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
			err = c.node.PaySubscription(res.SenderAddress, res.ReceiverAddress, res.StreamID, res.Amount)
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
	SenderAddress     string         `json:"senderAddress"`
	SenderPublicKey   *rsa.PublicKey `json:"senderPublicKey"`
	ReceiverAddress   string         `json:"receiverAddress"`
	ReceiverPublicKey *rsa.PublicKey `json:"receiverPublicKey"`
	StreamID          string         `json:"streamID"`
	Amount            float64        `json:"amount"`
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
				res.SenderAddress, res.SenderPublicKey,
				res.ReceiverAddress, res.ReceiverPublicKey, res.StreamID, res.Amount,
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
	SenderAddress   string  `json:"senderAddress"`
	ReceiverAddress string  `json:"receiverAddress"`
	StreamID        string  `json:"streamID"`
	Amount          float64 `json:"amount"`
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
				res.SenderAddress,
				res.ReceiverAddress,
				res.StreamID,
				res.Amount,
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
	PublicKey *rsa.PublicKey `json:"publicKey"`
	Address   string         `json:"address"`
	Amount    float64        `json:"amount"`
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

			err = c.node.PutInitialBlockOnChain(res.PublicKey, res.Address, res.Amount)
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

func (c pki) SendPrivateMessage() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := SendPrivateMessageBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal addPeerArgument: "+err.Error(),
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

			_, err = c.node.SendEncryptedMsg(transportMsg, res.To)
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

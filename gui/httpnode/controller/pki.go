package controller

import (
	"encoding/json"
	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"io"
	"net/http"
)

// NewCrowds returns a new initialized crowds.
func NewCrowds2(node peer.Peer, log *zerolog.Logger) crowds {
	return crowds{
		node: node,
		log:  log,
	}
}

type crowds2 struct {
	node peer.Peer
	log  *zerolog.Logger
}

type CrowdsSendBody2 struct {
	to    string
	body  string
	peers []string
}
type CrowdsDownloadBody2 struct {
	filename string
	peers    []string
}

func (c crowds) CrowdsSend2() http.HandlerFunc {
	// pozvati ficinu fju za registrovanje poruke da andrija ne pamti na frontu - opciono

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := CrowdsSendBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed to unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}

			c.node.CrowdsSend(res.peers, res.body, res.to)

		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (c crowds) CrowdsDownload2() http.HandlerFunc {
	// pozvati ficinu fju za registrovanje poruke da andrija ne pamti na frontu - opciono

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := CrowdsDownloadBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed to unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}

			c.node.CrowdsDownload(res.peers, res.filename)

		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

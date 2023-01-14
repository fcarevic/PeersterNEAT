package controller

import (
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"io"
	"net/http"
)

// NewCrowds returns a new initialized crowds.
func NewCrowds(node peer.Peer, log *zerolog.Logger) crowds {
	return crowds{
		node: node,
		log:  log,
	}
}

type crowds struct {
	node peer.Peer
	log  *zerolog.Logger
}

type CrowdsSendBody struct {
	To    string   `json:"to"`
	Body  string   `json:"body"`
	Peers []string `json:"peers"`
}
type CrowdsDownloadBody struct {
	Filename string   `json:"filename"`
	Peers    []string `json:"peers"`
}

func (c crowds) CrowdsSend() http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := CrowdsSendBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}
			err = c.node.CrowdsSend(res.Peers, res.Body, res.To)
			if err != nil {
				http.Error(w, "failed To send crowds message: "+err.Error(), http.StatusInternalServerError)
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

func (c crowds) CrowdsDownload() http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := CrowdsDownloadBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}

			fmt.Println(res)
			_, err = c.node.CrowdsDownload(res.Peers, res.Filename)
			if err != nil {
				http.Error(w, "failed to download with crowds: "+err.Error(), http.StatusInternalServerError)
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

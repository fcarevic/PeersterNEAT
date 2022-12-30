package controller

import (
	"encoding/base64"
	"encoding/json"
	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"io"
	"net/http"
)

const VideoPath = "/home/andrijajelenkovic/Documents/EPFL/dse/cs438-2022-hw3-student-037/gui/web/assets/hlsVideo"

type streaming struct {
	node peer.Peer
	log  *zerolog.Logger
}

type AnnounceStreamBody struct {
	name  string
	price uint
	image string
}

type StartStreamBody struct {
	name         string
	price        uint
	streamId     string
	manifestName string
	dir          string
	image        string
}

func NewStreaming(node peer.Peer, log *zerolog.Logger) streaming {
	return streaming{
		node: node,
		log:  log,
	}
}

func (s streaming) AnnounceStream() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := AnnounceStreamBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed to unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}

			image, err := base64.StdEncoding.DecodeString(res.image)
			if err != nil {
				http.Error(w, "error decoding image", http.StatusInternalServerError)
			}

			streamId, err := s.node.AnnounceStartStreaming(res.name, res.price, image)
			if err != nil {
				http.Error(w, "error announcing stream", http.StatusInternalServerError)
			}
			w.Write([]byte("\"" + streamId + "\""))
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (s streaming) StartStream() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := StartStreamBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed to unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}

			image, err := base64.StdEncoding.DecodeString(res.image)
			if err != nil {
				http.Error(w, "error decoding image", http.StatusInternalServerError)
			}

			s.node.StreamFFMPG4(res.manifestName, res.dir, res.name, res.price, res.streamId, image)
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (s streaming) ConnectToStream() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			streamId := r.URL.Query().Get("streamId")
			streamerId := r.URL.Query().Get("streamerId")
			s.node.ConnectToStream(streamId, streamerId)
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (s streaming) ReceiveStream() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			streamId := r.URL.Query().Get("streamId")
			err := s.node.ReceiveFFMPG4(streamId, VideoPath)
			if err != nil {
				http.Error(w, "failed receiving stream", http.StatusBadRequest)
			}
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

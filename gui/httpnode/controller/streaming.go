package controller

import (
	"encoding/base64"
	"encoding/json"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"io"
	"net/http"
	"os"
)

const VideoPath = "./web/assets/hlsVideo"

type streaming struct {
	node peer.Peer
	log  *zerolog.Logger
}

type AnnounceStreamBody struct {
	Name  string
	Price uint
	Image string
}

type StartStreamBody struct {
	Name         string
	Price        uint
	StreamID     string
	ManifestName string
	Dir          string
	Image        string
}

type Stream struct {
	StreamID   string  `json:"streamId"`
	Name       string  `json:"name"`
	Price      uint    `json:"price"`
	Grade      float64 `json:"grade"`
	Viewers    uint    `json:"viewers"`
	Image      string  `json:"image"`
	StreamerID string  `json:"streamerId"`
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
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := AnnounceStreamBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal announceStream: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}

			var image []byte
			if res.Image != "" {
				image, err = base64.StdEncoding.DecodeString(res.Image)
				if err != nil {
					http.Error(w, "error decoding Image", http.StatusInternalServerError)
				}
			}

			streamID, err := s.node.AnnounceStartStreaming(res.Name, res.Price, image)
			if err != nil {
				http.Error(w, "error announcing stream", http.StatusInternalServerError)
			}
			w.Write([]byte("\"" + streamID + "\""))
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
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
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := StartStreamBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal addPeerArgument: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}

			var image []byte
			if res.Image != "" {
				image, err = base64.StdEncoding.DecodeString(res.Image)
				if err != nil {
					http.Error(w, "error decoding Image", http.StatusInternalServerError)
				}
			}

			go s.node.StreamFFMPG4(res.ManifestName, res.Dir, res.Name, res.Price, res.StreamID, image)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (s streaming) StopStream() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			streamID := r.URL.Query().Get("streamId")
			err := s.node.AnnounceStopStreaming(streamID)
			if err != nil {
				http.Error(w, "error stopping stream", http.StatusInternalServerError)
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

func (s streaming) ConnectToStream() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			streamID := r.URL.Query().Get("streamId")
			streamerID := r.URL.Query().Get("streamerId")
			err := s.node.ConnectToStream(streamID, streamerID)
			if err != nil {
				http.Error(w, "error connecting To stream", http.StatusInternalServerError)
			}
			CreateFFMPG4Header(streamID, VideoPath)
			err = s.node.ReceiveFFMPG4(streamID, VideoPath)
			if err != nil {
				http.Error(w, "error listening To stream", http.StatusInternalServerError)
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
func CreateFFMPG4Header(streamID string, dir string) {
	metafileHeader := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:13\n#EXT-X-MEDIA-SEQUENCE:0\n"
	errWrite := os.WriteFile(dir+"/"+streamID+".m3u8", []byte(metafileHeader), 0666)
	log.Info().Msgf("Created metafile: " + dir + "/" + streamID + ".m3u8")
	if errWrite != nil {
		log.Error().Msgf(
			"Error while creating metafile for %s : %s",
			streamID,
			errWrite.Error(),
		)
		return
	}
}

func (s streaming) ReceiveStream() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			streamID := r.URL.Query().Get("StreamId")
			err := s.node.ReceiveFFMPG4(streamID, VideoPath)
			if err != nil {
				http.Error(w, "failed receiving stream", http.StatusBadRequest)
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

func (s streaming) GetStreams() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			streamInfos := s.node.GetAllStreams()
			streams := make([]Stream, 0)
			for _, streamInfo := range streamInfos {
				log.Info().Msgf("StreamerId: " + streamInfo.StreamerID)
				streams = append(
					streams, Stream{
						StreamID:   streamInfo.StreamID,
						Name:       streamInfo.Name,
						Price:      streamInfo.Price,
						Grade:      streamInfo.Grade,
						Viewers:    streamInfo.CurrentlyWatching,
						Image:      base64.StdEncoding.EncodeToString(streamInfo.Thumbnail),
						StreamerID: streamInfo.StreamerID,
					},
				)
			}

			js, err := json.Marshal(streams)
			if err != nil {
				http.Error(w, "error marshalling streamInfo", http.StatusInternalServerError)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Write(js)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

type ReactToStreamBody struct {
	StreamID   string  `json:"streamId"`
	StreamerID string  `json:"streamerId"`
	Grade      float64 `json:"grade"`
}

func (s streaming) ReactToStream() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			buf, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed To read Body: "+err.Error(), http.StatusInternalServerError)
				return
			}

			res := ReactToStreamBody{}
			err = json.Unmarshal(buf, &res)
			if err != nil {
				http.Error(
					w, "failed To unmarshal reactToStream body: "+err.Error(),
					http.StatusInternalServerError,
				)
				return
			}
			err = s.node.ReactToStream(res.StreamID, res.StreamerID, res.Grade)
			if err != nil {
				http.Error(w, "failed to react to stream: "+err.Error(), http.StatusInternalServerError)
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

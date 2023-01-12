package controller

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"io"
	"net/http"
	"os"
)

const VideoPath = "/home/andrijajelenkovic/Documents/EPFL/dse/PeersterNEAT/gui/web/assets/hlsVideo"

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
	StreamId     string
	ManifestName string
	Dir          string
	Image        string
}

type Stream struct {
	StreamId   string  `json:"streamId"`
	Name       string  `json:"name"`
	Price      uint    `json:"price"`
	Grade      float64 `json:"grade"`
	Viewers    uint    `json:"viewers"`
	Image      string  `json:"image"`
	StreamerId string  `json:"streamerId"`
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

			streamId, err := s.node.AnnounceStartStreaming(res.Name, res.Price, image)
			if err != nil {
				http.Error(w, "error announcing stream", http.StatusInternalServerError)
				fmt.Print("erro announce: %s", err.Error())
			} else {
				fmt.Println("announced streaming")
			}
			fmt.Print("Ovde")
			w.Write([]byte("\"" + streamId + "\""))
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

			go s.node.StreamFFMPG4(res.ManifestName, res.Dir, res.Name, res.Price, res.StreamId, image)
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
			streamId := r.URL.Query().Get("streamId")
			err := s.node.AnnounceStopStreaming(streamId)
			if err != nil {
				fmt.Println(err.Error())
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
			streamId := r.URL.Query().Get("streamId")
			streamerId := r.URL.Query().Get("streamerId")
			err := s.node.ConnectToStream(streamId, streamerId)
			if err != nil {
				fmt.Printf("%v\n", err.Error())
				http.Error(w, "error connecting To stream", http.StatusInternalServerError)
			}
			CreateFFMPG4Header(streamId, VideoPath)
			err = s.node.ReceiveFFMPG4(streamId, VideoPath)
			if err != nil {
				fmt.Printf("%v\n", err.Error())
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
	fmt.Println("Created metafile: " + dir + "/" + streamID + ".m3u8")
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
			streamId := r.URL.Query().Get("StreamId")
			err := s.node.ReceiveFFMPG4(streamId, VideoPath)
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
				fmt.Println("StreamerId: " + streamInfo.StreamerID)
				streams = append(
					streams, Stream{
						StreamId:   streamInfo.StreamID,
						Name:       streamInfo.Name,
						Price:      streamInfo.Price,
						Grade:      streamInfo.Grade,
						Viewers:    streamInfo.CurrentlyWatching,
						Image:      base64.StdEncoding.EncodeToString(streamInfo.Thumbnail),
						StreamerId: streamInfo.StreamerID,
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
	StreamId   string  `json:"streamId"`
	StreamerId string  `json:"streamerId"`
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
			err = s.node.ReactToStream(res.StreamId, res.StreamerId, res.Grade)
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

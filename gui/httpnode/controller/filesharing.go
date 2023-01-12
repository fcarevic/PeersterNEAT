package controller

import (
	"encoding/json"
	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"net/http"
	"regexp"
)

type filesharing struct {
	conf peer.Configuration
	log  *zerolog.Logger
}

func NewFileSharing(conf peer.Configuration, log *zerolog.Logger) filesharing {
	return filesharing{
		conf: conf,
		log:  log,
	}
}

type LocalFile struct {
	Metahash string `json:"metahash"`
	Type     string `json:"type"`
}

func (f filesharing) GetLocalFilesHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			blobStore := f.conf.Storage.GetDataBlobStore()
			files := make([]LocalFile, 0)
			regex, err := regexp.Compile("([a-z0-9]{64}\\n)*[a-z0-9]{64}$")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			blobStore.ForEach(
				func(key string, val []byte) bool {
					if regex.Match(val) {
						files = append(
							files, LocalFile{
								Metahash: key,
								Type:     "Metafile",
							},
						)
					} else {
						files = append(
							files, LocalFile{
								Metahash: key,
								Type:     "Chunk",
							},
						)
					}

					return true
				},
			)
			js, err := json.Marshal(files)
			if err != nil {
				http.Error(w, "error marshalling hashes", http.StatusInternalServerError)
			}
			w.Write(js)
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

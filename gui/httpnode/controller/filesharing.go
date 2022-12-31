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

func (f filesharing) GetLocalFilesHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			blobStore := f.conf.Storage.GetDataBlobStore()
			hashes := make([]string, 0)
			regex, err := regexp.Compile("([a-z0-9]{64}\\n)*[a-z0-9]{64}$")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			blobStore.ForEach(
				func(key string, val []byte) bool {
					if regex.Match(val) {
						hashes = append(hashes, key)
					}

					return true
				},
			)
			js, err := json.Marshal(hashes)
			if err != nil {
				http.Error(w, "error marshalling hashes", http.StatusInternalServerError)
			}
			w.Write(js)
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

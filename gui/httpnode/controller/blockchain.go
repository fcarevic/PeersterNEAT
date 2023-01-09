package controller

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"text/template"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/types"
)

// NewBlockchain returns a new initialized blockchain.
func NewBlockchain(conf peer.Configuration, log *zerolog.Logger) blockchain {
	return blockchain{
		conf: conf,
		log:  log,
	}
}

type blockchain struct {
	conf peer.Configuration
	log  *zerolog.Logger
}

func (b blockchain) BlockchainHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			b.blockchainGet(w, r)
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (b blockchain) GetFilesOnBlockchainHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			store := b.conf.Storage.GetBlockchainStore()

			//TODO: Correct To show only file blocks
			fileBlocks, err := b.getBlocks(store)
			if err != nil {
				http.Error(w, "failed To unmarshal block: "+err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")

			js, err := json.MarshalIndent(&fileBlocks, "", "\t")
			if err != nil {
				http.Error(
					w, fmt.Sprintf("failed To marshal catalog: %v", err),
					http.StatusInternalServerError,
				)
				return
			}

			w.Write(js)
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (b blockchain) blockchainGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	store := b.conf.Storage.GetBlockchainStore()

	blocks, err := b.getBlocks(store)
	if err != nil {
		http.Error(w, "failed To unmarshal block: "+err.Error(), http.StatusInternalServerError)
		return
	}
	viewData := struct {
		NodeAddr      string
		LastBlockHash string
		Blocks        []viewBlock
	}{
		NodeAddr:      b.conf.Socket.GetAddress(),
		LastBlockHash: hex.EncodeToString(store.Get(storage.LastBlockKey)),
		Blocks:        blocks,
	}

	tmpl, err := template.New("html").ParseFiles(("httpnode/controller/blockchain.gohtml"))
	if err != nil {
		http.Error(w, "failed To parse template: "+err.Error(), http.StatusInternalServerError)
		return
	}

	tmpl.ExecuteTemplate(w, "blockchain.gohtml", viewData)
}

func (b blockchain) getBlocks(store storage.Store) ([]viewBlock, error) {
	lastBlockHashHex := hex.EncodeToString(store.Get(storage.LastBlockKey))
	endBlockHasHex := hex.EncodeToString(make([]byte, 32))

	if lastBlockHashHex == "" {
		lastBlockHashHex = endBlockHasHex
	}

	blocks := []viewBlock{}

	for lastBlockHashHex != endBlockHasHex {
		lastBlockBuf := store.Get(string(lastBlockHashHex))

		var lastBlock types.BlockchainBlock

		err := lastBlock.Unmarshal(lastBlockBuf)
		if err != nil {
			return nil, err
		}

		blocks = append(
			blocks, viewBlock{
				Index:    lastBlock.Index,
				Hash:     hex.EncodeToString(lastBlock.Hash),
				ValueID:  lastBlock.Value.UniqID,
				Name:     lastBlock.Value.Filename,
				Metahash: lastBlock.Value.Metahash,
				PrevHash: hex.EncodeToString(lastBlock.PrevHash),
			},
		)

		lastBlockHashHex = hex.EncodeToString(lastBlock.PrevHash)
	}

	return blocks, nil
}

func (b blockchain) getFileBlocks(store storage.Store) ([]viewBlock, error) {
	blocks, err := b.getBlocks(store)
	if err != nil {
		return nil, err
	}

	fileBlocks := make([]viewBlock, 0)
	for _, block := range blocks {
		if !strings.Contains(block.Metahash, "BEGIN PUBLIC KEY") {
			fmt.Println(block.Metahash)
			fileBlocks = append(fileBlocks, block)
		}
	}

	return fileBlocks, nil
}

type viewBlock struct {
	Index    uint
	Hash     string
	ValueID  string
	Name     string
	Metahash string
	PrevHash string
}

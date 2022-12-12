package impl

import (
	"bufio"
	"encoding/hex"
	"go.dedis.ch/cs438/peer"
	"log"
	"os"
	"strings"
	"time"
)

const ROOTDIR = "/home/andrijajelenkovic/Documents/EPFL/dse/PeersterNEAT/video"

func encryptSymmetricKey(symmetricKey []byte, keyID string) ([]byte, error) {
	return symmetricKey, nil
}

func decryptSymmetricKey(encryptedSymmetricKey []byte, keyID string) ([]byte, error) {
	return encryptedSymmetricKey, nil
}

func (s *StreamInfo) getGrade(streamID string) (float64, error) {
	return 0, nil
}

func (n *node) streamFFMPG4(manifestName string, dir string, name string, price uint, streamID string) {
	file, err := os.Open(dir + "/" + manifestName)
	defer file.Close()
	s := bufio.NewScanner(file)
	s.Split(bufio.ScanLines)
	if err != nil {
		log.Println(err)
		return
	}
	for s.Scan() {
		command := s.Text()
		if command != "#EXT-X-ENDLIST" {
			s.Scan()
			fileName := s.Text()
			chunk, err := os.ReadFile(dir + "/" + fileName)
			if err != nil {
				log.Println(err)
				return
			}
			encoded := hex.EncodeToString(chunk)
			encoded = command + peer.MetafileSep + encoded
			err = n.Stream(strings.NewReader(encoded), name, price, streamID)
			if err != nil {
				return
			}
			time.Sleep(time.Second)
		}

		log.Println(n)

		if err != nil {
			log.Println(err)
		}
	}
}

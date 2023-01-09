package impl

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/types"
	"os"
	"strings"
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

func (n *node) StreamFFMPG4(
	manifestName string, dir string, name string, price uint, streamID string,
	thumbnail []byte,
) {
	file, err := os.Open(dir + "/" + manifestName)
	if err != nil {
		log.Error().Msgf("%s", err.Error())
		return
	}
	fmt.Println("asd")
	log.Error().Msgf("Here")
	defer file.Close()
	s := bufio.NewScanner(file)
	s.Split(bufio.ScanLines)

	lines := -1
	for s.Scan() {
		lines++
		fmt.Println(lines)
		command := s.Text()
		encoded := ""
		if command == "#EXT-X-ENDLIST" {
			encoded = command
		} else if strings.Contains(command, "EXTINF") {
			s.Scan()
			fileName := s.Text()
			chunk, err := os.ReadFile(dir + "/" + fileName)
			if err != nil {
				log.Error().Msgf("%s", err.Error())
				return
			}
			encoded = hex.EncodeToString(chunk)
			encoded = command + peer.MetafileSep + fileName + peer.MetafileSep + encoded
		} else {
			s.Scan()
			nextLine := s.Text()
			encoded = command + peer.MetafileSep + nextLine
		}
		log.Info().Msgf("lines %d", lines)
		err = n.Stream(strings.NewReader(encoded), name, price, streamID, thumbnail, uint(lines))
		if err != nil {
			return
		}
		//time.Sleep(time.Second)

		log.Error().Msgf("%d", n)

		if err != nil {
			log.Error().Msgf("%s", err.Error())
		}
	}
	fmt.Println("finished")
}

func (n *node) ReceiveFFMPG4(streamID string, dir string) error {
	n.activeThreads.Add(1)
	go func(streamID string, dir string) {
		defer n.activeThreads.Done()
		channel := make(chan types.StreamMessage, 5000)
		log.Info().Msgf("Starting  FFMPG4 Listener")
		err := n.streamInfo.registerFFMPG4Channel(streamID, channel)
		if err != nil {
			log.Error().Msgf("%v\n", err.Error())
			log.Error().Msgf("Error while registering to a stream: %s", n.conf.Socket.GetAddress())
			return
		}

		log.Info().Msgf("Started  FFMPG4 Listener")
		for {
			select {
			case <-n.notifyEnd:
				return
			case msg, ok := <-channel:
				if !ok {
					return
				}
				log.Info().Msgf("Received message")
				decodeFFMPG4StreamMessage(msg, dir, streamID)
			}
			//chunks, err := n.GetNextChunks(streamID, -1)
			//if err != nil {
			//	return err
			//}
		}
	}(streamID, dir)
	return nil
}

func decodeFFMPG4StreamMessage(chunk types.StreamMessage, dir string, streamID string) {
	encoded := string(chunk.Data.Chunk)
	splitted := strings.Split(encoded, peer.MetafileSep)
	command := splitted[0]
	toWrite := ""
	if command == "#EXT-X-ENDLIST" {
		toWrite = command
	} else if strings.Contains(command, "EXTINF") {
		filename := splitted[1]
		dataDec, errDec := hex.DecodeString(splitted[2])
		if errDec != nil {
			log.Error().Msgf("Error while decoding data chunk in recevied FFMPG4 %s", errDec.Error())
			return
		}
		errWrite := os.WriteFile(dir+"/"+filename, dataDec, 0666)
		fmt.Println(dir + "/" + filename)
		if errWrite != nil {
			log.Error().Msgf(
				"Error while writing data to a file %s in recevied FFMPG4 %s",
				filename,
				errWrite.Error(),
			)
			return
		}

		toWrite = command + "\n" + filename + "\n"
		log.Info().Msgf("Received StreamID: %s \t filename: %s:\t", streamID[:4], filename)
	} else {
		toWrite = command + "\n" + splitted[1] + "\n"
	}

	// This is metafile
	f, errOpenMetaFile := os.OpenFile(dir+"/"+streamID+".m3u8", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if errOpenMetaFile != nil {
		log.Error().Msgf(
			"Error while wiritng data to a file %s in received FFMPG4 %s",
			command, errOpenMetaFile.Error(),
		)
	}

	if _, errCmdWrite := f.WriteString(toWrite); errCmdWrite != nil {
		if errCmdWrite != nil {
			log.Error().Msgf(
				"Error while wiritng data to a file %s in received FFMPG4 %s",
				toWrite, errCmdWrite.Error(),
			)
		}
	}

	errClose := f.Close()
	if errClose != nil {
		log.Error().Msgf("Error while writing data to a file %s in received FFMPG4 %s", toWrite, errClose.Error())
	}
}

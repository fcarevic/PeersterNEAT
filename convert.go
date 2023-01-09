package main

import (
	"log"
	"os/exec"
)

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Println("conversion in progress")
		}
	}()
	err := exec.Command(
		"ffmpeg",
		"-i",
		"./video/filename.mp4",
		"-codec:",
		"copy",
		"-start_number",
		"0",
		"-hls_time",
		"10",
		"-hls_list_size",
		"0",
		"-f",
		"hls",
		"./video/cetvhgn13hoh536ron4g.m3u8",
	).Run()

	log.Println(err.Error())
}

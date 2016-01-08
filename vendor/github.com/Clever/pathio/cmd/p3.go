package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/Clever/pathio"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	action    = kingpin.Arg("action", "S3 action: `upload` or `download`").Required().String()
	s3Path    = kingpin.Arg("s3_path", "S3 path to upload/download").Required().String()
	localPath = kingpin.Arg("local_path", "local file to upload or write to").Required().String()
)

func main() {
	kingpin.Parse()
	if *action == "upload" {
		file, err := os.Open(*localPath)
		if err != nil {
			log.Fatalf("Error opening file to upload: %s", err)
		}
		defer file.Close()

		err = pathio.WriteReader(*s3Path, file)
		if err != nil {
			log.Fatalf("Error uploading file: %s", err)
		}
		fmt.Printf("File uploaded to %s\n", *s3Path)

	} else if *action == "download" {
		file, err := os.Create(*localPath)
		if err != nil {
			log.Fatalf("Error creating local file: %s", err)
		}
		defer file.Close()

		reader, err := pathio.Reader(*s3Path)
		if err != nil {
			log.Fatalf("Failed to find s3 file: %s", err)
		}
		defer reader.Close()
		_, err = io.Copy(file, reader)
		if err != nil {
			log.Fatalf("Failed to download and write s3 file: %s", err)
		}
		fmt.Printf("Downloaded %s to %s\n", *s3Path, *localPath)

	} else {
		log.Fatalf("Unknown action: '%s'. Must be either 'upload' or 'download'.", *action)
	}
}

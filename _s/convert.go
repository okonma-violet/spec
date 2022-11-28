package main

import (
	"log"
	"os/exec"
	"strings"
)

func run(path string, args []string, debug bool) (out string, err error) {

	cmd := exec.Command(path, args...)

	var b []byte
	b, err = cmd.CombinedOutput()
	out = string(b)

	if debug {
		log.Println(strings.Join(cmd.Args[:], " "))

		if err != nil {
			log.Println("RunCMD ERROR")
			log.Println(out)
		}
	}

	return
}

func converttocsv(filename string) error {
	_, err := run("libreoffice", []string{"--convert-to", "csv", "--infilter=CSV:44,34,76,1", filename}, true)
	return err
}

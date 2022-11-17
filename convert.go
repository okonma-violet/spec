package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"

	"golang.org/x/text/encoding/charmap"
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

func unzip(filename string) error {
	_, err := run("unzip", []string{filename}, true)
	return err
}

func converttocsv(filename string) error {
	_, err := run("libreoffice", []string{"--convert-to", "csv", "--infilter=CSV:44,34,76,1", filename}, true)
	return err
}

func encode(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	decoder := charmap.Windows1251.NewDecoder()
	reader := decoder.Reader(f)
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	f.Close()
	return ioutil.WriteFile(filename, b, os.ModePerm)
}

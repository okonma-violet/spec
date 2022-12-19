package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/exec"
	"os/signal"

	"strings"
	"syscall"
	"time"

	"github.com/okonma-violet/confdecoder"
	"github.com/okonma-violet/spec/logs/encode"
	"github.com/okonma-violet/spec/logs/logger"
)

type config struct {
	ZipPath      string
	CsvPath      string
	TimerSeconds int

	ShittyCharsetZipNamesPrefixes []string
	ShittyCharsets                []string
}

type shittycharsetzip struct {
	prefix  string
	charset string
}

const unzippath = "./unzipped/"

func main() {
	conf := &config{}
	err := confdecoder.DecodeFile("config.txt", conf)
	if err != nil {
		panic("read config file err: " + err.Error())
	}
	if conf.ZipPath == "" {
		panic("no ZipPath specified in config.txt")
	}
	if conf.CsvPath == "" {
		panic("no CsvPath specified in config.txt")
	}
	if conf.TimerSeconds == 0 {
		panic("no TimerSeconds specified in config.txt or is zero")
	}
	if len(conf.ShittyCharsetZipNamesPrefixes) != len(conf.ShittyCharsets) {
		panic("lengths mismatch of ShittyCharsetZipNamesPrefixes with ShittyCharsets in config.txt")
	}
	conf.ZipPath += "/"
	conf.CsvPath += "/"

	rp := flag.Bool("r", false, "remove processed zip and xls files")
	flag.Parse()

	shittycharsets := make([]shittycharsetzip, 0, len(conf.ShittyCharsetZipNamesPrefixes))
	for i := 0; i < len(conf.ShittyCharsetZipNamesPrefixes); i++ {
		shittycharsets = append(shittycharsets, shittycharsetzip{prefix: strings.ToLower(conf.ShittyCharsetZipNamesPrefixes[i]), charset: conf.ShittyCharsets[i]})
	}

	ctx, cancel := createContextWithInterruptSignal()

	flsh := logger.NewFlusher(encode.DebugLevel)
	l := flsh.NewLogsContainer("unzipper")

	go func() {
		//ticker := time.NewTicker(time.Second * time.Duration(conf.TimerSeconds))
		l.Debug("Job", "started")
		do_job(l, cancel, *rp, conf, shittycharsets)
		l.Debug("Job", "done, sleeping")
		cancel()
		// for {
		// 	select {
		// 	case <-ctx.Done():
		// 		l.Info("Routine", "context done, exiting loop")
		// 		return
		// 	case <-ticker.C:
		// 		l.Debug("Job", "started")
		// 		do_job(l, cancel, conf)
		// 		l.Debug("Job", "done, sleeping")
		// 	}

		// }
	}()

	<-ctx.Done()
	l.Debug("Context", "done, exiting")
	flsh.Close()
	flsh.DoneWithTimeout(time.Second * 5)
}

func do_job(l logger.Logger, cancel context.CancelFunc, remove_processed bool, conf *config, shittyzips []shittycharsetzip) {
	l.Debug("ZipDir_Loop", "started")
	files, err := os.ReadDir(conf.ZipPath)
	if err != nil {
		l.Error("ZipDir_Loop/ReadDir", err)
		cancel()
		return
	}

	for _, f := range files {
		fname_lowered := strings.ToLower(f.Name())
		if f.IsDir() {
			continue
		}

		if strings.HasSuffix(fname_lowered, ".zip") {
			for i := 0; i < len(shittyzips); i++ {
				if strings.HasPrefix(fname_lowered, shittyzips[i].prefix) {
					if out, err := unzip_with_charset(conf.ZipPath+f.Name(), shittyzips[i].charset, unzippath); err != nil {
						l.Error("Unzip", errors.New(err.Error()+", out: "+out))
						continue
					}
					l.Debug("Unzip", "unzipped "+f.Name())
					goto remove
				}
			}
			if out, err := unzip(conf.ZipPath+f.Name(), unzippath); err != nil {
				l.Error("Unzip", errors.New(err.Error()+" \nout: "+out))
				continue
			}
			l.Debug("Unzip", "unzipped "+f.Name())
			goto remove
		}

		if strings.Contains(fname_lowered, ".xls") {
			if out, err := converttocsv(conf.ZipPath + f.Name()); err != nil {
				l.Error("ConvertToCsv", errors.New(err.Error()+" \nout: "+out))
				continue
			}
			l.Debug("ConvertToCsv", "converted "+f.Name())
			goto remove
		}

		if strings.HasSuffix(f.Name(), ".csv") {
			if err = os.Rename(conf.ZipPath+f.Name(), conf.CsvPath+f.Name()); err != nil {
				l.Error("MoveCsv/Rename", err)
				continue
			}
			l.Debug("MoveCsv", "moved "+f.Name())
			continue
		}
		l.Warning("ZipDir_Loop", "nondir/nonzip/noncsv/nonxls file found: "+f.Name())
		continue
	remove:
		if remove_processed {
			if err = os.Remove(conf.ZipPath + f.Name()); err != nil {
				l.Error("ZipDir_Loop/Remove", err)
			}
			l.Debug("ZipDir_Loop", "removed "+f.Name())
		}
	}
	l.Debug("ZipDir_Loop", "done")

	l.Debug("UnzippedDir_Loop", "started")
	files, err = os.ReadDir(unzippath)
	if err != nil {
		l.Error("UnzippedDir_Loop/ReadDir", err)
		cancel()
		return
	}
	for _, f := range files {
		fname_lowered := strings.ToLower(f.Name())
		if f.IsDir() {
			continue
		}

		if strings.Contains(fname_lowered, ".xls") {
			if out, err := converttocsv(unzippath + f.Name()); err != nil {
				l.Error("ConvertToCsv", errors.New(err.Error()+" \nout: "+out))
				continue
			}
			l.Debug("ConvertToCsv", "converted "+f.Name())
			goto remove2
		}

		if strings.HasSuffix(f.Name(), ".csv") {
			if err = os.Rename(unzippath+f.Name(), conf.CsvPath+f.Name()); err != nil {
				l.Error("MoveCsv/Rename", err)
				continue
			}
			l.Debug("MoveCsv", "moved "+f.Name())
			continue
		}
		l.Warning("UnzippedDir_Loop", "nondir/noncsv/nonxls file found: "+f.Name())
		continue
	remove2:
		if remove_processed {
			if err = os.Remove(unzippath + f.Name()); err != nil {
				l.Error("UnzippedDir_Loop/Remove", err)
			}
			l.Debug("UnzippedDir_Loop", "removed "+f.Name())
		}
	}
	l.Debug("UnzippedDir_Loop", "done")

	l.Debug("ConvertedToCsvDir_Loop", "started")
	files, err = os.ReadDir(".")
	if err != nil {
		l.Error("ConvertedToCsvDir_Loop/ReadDir", err)
		cancel()
		return
	}
	for _, f := range files {
		fname_lowered := strings.ToLower(f.Name())
		if f.IsDir() {
			continue
		}
		if !f.IsDir() && strings.HasSuffix(fname_lowered, ".csv") {
			if err = os.Rename(f.Name(), conf.CsvPath+f.Name()); err != nil {
				l.Error("Movecsv/Rename", err)
				continue
			}
			l.Debug("Movecsv", "moved "+f.Name())
		}
	}
	l.Debug("ConvertedToCsvDir_Loop", "done")
}

func createContextWithInterruptSignal() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		cancel()
	}()
	return ctx, cancel
}
func run(path string, args []string) (out string, err error) {

	cmd := exec.Command(path, args...)

	var b []byte
	b, err = cmd.CombinedOutput()
	out = string(b)

	return
}

func unzip(filename, dir string) (string, error) {
	return run("unzip", []string{"-o", filename, "-d", dir})
}
func unzip_with_charset(filename, charset, dir string) (string, error) {
	return run("unzip", []string{"-O", charset, "-o", filename, "-d", dir})
}
func converttocsv(filename string) (string, error) {
	return run("soffice", []string{"--headless", "--convert-to", "csv", "--infilter=CSV:44,34,76,1", filename})
}

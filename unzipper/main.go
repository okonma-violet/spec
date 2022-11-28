package main

import (
	"context"
	"errors"
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
}

const unzippedpath = "./unzipped/"

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
	conf.ZipPath += "/"
	conf.CsvPath += "/"

	ctx, cancel := createContextWithInterruptSignal()

	flsh := logger.NewFlusher(encode.DebugLevel)
	l := flsh.NewLogsContainer("unzipper")

	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(conf.TimerSeconds))
		l.Debug("Job", "started")
		do_job(l, cancel, conf)
		l.Debug("Job", "done, sleeping")
		for {
			select {
			case <-ctx.Done():
				l.Info("Routine", "context done, exiting loop")
				return
			case <-ticker.C:
				l.Debug("Job", "started")
				do_job(l, cancel, conf)
				l.Debug("Job", "done, sleeping")
			}

		}
	}()

	<-ctx.Done()
	l.Debug("Context", "done, exiting")
	flsh.Close()
	flsh.DoneWithTimeout(time.Second * 5)
}

func do_job(l logger.Logger, cancel context.CancelFunc, conf *config) {
	l.Debug("Unzip", "started")
	files, err := os.ReadDir(conf.ZipPath)
	if err != nil {
		l.Error("Unzip/ReadDir", err)
		cancel()
		return
	}

	for _, f := range files {
		fname_lowered := strings.ToLower(f.Name())
		if f.IsDir() || !strings.HasSuffix(fname_lowered, ".zip") {
			l.Warning("Unzip/ReadDir", "nonzip file founded: "+f.Name())
			continue
		}
		if out, err := unzip(conf.ZipPath+f.Name(), unzippedpath); err != nil {
			l.Error("Unzip", errors.New(err.Error()+" \nout: "+out))
			continue
		}
		l.Debug("Unzip", "unzipped "+f.Name())
		// if err = os.Remove(conf.ZipPath + f.Name()); err != nil {
		// 	l.Error("Unzip/Remove", err)
		// }
		// l.Debug("Unzip", "removed "+f.Name())
	}
	l.Debug("Unzip", "done")

	l.Debug("ConvertToCsv", "started")
	files, err = os.ReadDir(unzippedpath)
	if err != nil {
		l.Error("ConvertToCsv/ReadDir", err)
		cancel()
		return
	}

	for _, f := range files {

		fname_lowered := strings.ToLower(f.Name())
		if f.IsDir() {
			l.Warning("ConvertToCsv/ReadDir", "dir founded "+f.Name())
			continue
		} else if strings.Contains(fname_lowered, ".xls") {
			if out, err := converttocsv(unzippedpath + f.Name()); err != nil {
				l.Error("ConvertToCsv", errors.New(err.Error()+" \nout: "+out))
				continue
			}
			l.Debug("ConvertToCsv", "converted "+f.Name())
			if err = os.Remove(unzippedpath + f.Name()); err != nil {
				l.Error("ConvertToCsv/Remove", err)
			}
			l.Debug("ConvertToCsv", "removed "+f.Name())
		} else if strings.Contains(fname_lowered, ".csv") {
			continue
		} else {
			l.Warning("ConvertToCsv/ReadDir", "nonxls && noncsv file founded "+f.Name())
		}
	}
	l.Debug("ConvertToCsv", "done")

	l.Debug("Movecsv", "started")
	files, err = os.ReadDir(".")
	if err != nil {
		l.Error("Movecsv/ReadDir", err)
		cancel()
		return
	}
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".csv") {
			if err = os.Rename(f.Name(), conf.CsvPath+f.Name()); err != nil {
				l.Error("Movecsv/Rename", err)
				continue
			}
			l.Debug("Movecsv", "moved ./"+f.Name())
		}
	}
	files, err = os.ReadDir(unzippedpath)
	if err != nil {
		l.Error("Movecsv/ReadDir", err)
		cancel()
		return
	}
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".csv") {
			if err = os.Rename(unzippedpath+f.Name(), conf.CsvPath+f.Name()); err != nil {
				l.Error("Movecsv/Rename", err)
				continue
			}
			l.Debug("Movecsv", "moved "+unzippedpath+f.Name())
		}
	}

	l.Debug("Movecsv", "done")
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

func unzip(filename string, dir string) (string, error) {
	return run("unzip", []string{"-o", filename, "-d", dir})

}
func converttocsv(filename string) (string, error) {
	return run("soffice", []string{"--headless", "--convert-to", "csv", "--infilter=CSV:44,34,76,1", filename})
}

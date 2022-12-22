package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/okonma-violet/confdecoder"
	"github.com/okonma-violet/spec/logs/encode"
	"github.com/okonma-violet/spec/logs/logger"
)

type config struct {
	DownloadsPath string
	TimerSeconds  int

	SuppliersConfsPath string
}

func main() {
	conf := &config{}
	err := confdecoder.DecodeFile("config.txt", conf)
	if err != nil {
		panic("read config file err: " + err.Error())
	}
	if conf.DownloadsPath == "" {
		panic("no RawCsvPath specified in config.txt")
	}
	if conf.SuppliersConfsPath == "" {
		panic("no CsvPath specified in config.txt")
	}
	if conf.TimerSeconds == 0 {
		panic("no TimerSeconds specified in config.txt or is zero")
	}

	conf.SuppliersConfsPath += "/"
	conf.DownloadsPath += "/"

	ctx, cancel := createContextWithInterruptSignal()

	flsh := logger.NewFlusher(encode.DebugLevel)
	l := flsh.NewLogsContainer("emailer")

	go func() {
		//ticker := time.NewTicker(time.Second * time.Duration(conf.TimerSeconds))
		l.Debug("Job", "started")
		sups, err := loadSuppliersConfigsFromDir(l, conf.SuppliersConfsPath)
		for i, s := range sups {
			fmt.Println(i)
			fmt.Println(s)
		}
		if err != nil {
			l.Error("LoadSuppliers", err)
			cancel()
			return
		}

		if err = checkMail(l, conf.DownloadsPath, sups); err != nil {
			l.Error("checkMail", err)
		}

		cancel()
		// for {
		// 	select {
		// 	case <-ctx.Done():
		// 		l.Info("Routine", "context done, exiting loop")
		// 		return
		// 	case <-ticker.C:
		// 		l.Debug("Job", "started")
		// 		sups, err = rep.GetSuppliersByNames()
		// 		if err != nil {
		// 			l.Error("GetSuppliersByNames", err)
		// 			l.Error("Job", errors.New("cant do without suppliers"))
		// 		} else {
		// 			conf.do_job(l, cancel, sups)
		// 			l.Debug("Job", "done, sleeping")
		// 		}
		// 	}

		// }
	}()

	<-ctx.Done()
	l.Debug("Context", "done, exiting")
	flsh.Close()
	flsh.DoneWithTimeout(time.Second * 5)
}

type supplier struct {
	Name                         string
	Email                        string
	MailFileNamePattern_Prefixes []string
	MailFileNamePattern_Suffixes []string
}

func loadSuppliersConfigsFromDir(l logger.Logger, path string) ([]supplier, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	sups := make([]supplier, 0, len(files))

	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".txt") {
			l.Warning("LoadSuppliers", "nontxt file founded: "+f.Name())
			continue
		}

		sfrm := supplier{}
		err := confdecoder.DecodeFile(path+f.Name(), &sfrm)
		if err != nil {
			return nil, errors.New("read supplier's config file err: " + err.Error())
		}
		if sfrm.Name == "" || sfrm.Email == "" {
			// l.Error("LoadSuppliers", errors.New("no data in supplier's config file: "+f.Name()))
			// continue
			return nil, errors.New("no data in supplier's config file: " + f.Name())
		}

		if len(sfrm.MailFileNamePattern_Prefixes) == 0 {
			if len(sfrm.MailFileNamePattern_Suffixes) == 0 {
				// l.Error("LoadSuppliers", errors.New("no mail files specified in config file: "+f.Name()))
				// continue
				return nil, errors.New("no mail files specified in config file: " + f.Name())
			} else {
				sfrm.MailFileNamePattern_Prefixes = make([]string, len(sfrm.MailFileNamePattern_Suffixes))
			}
		}
		if len(sfrm.MailFileNamePattern_Suffixes) == 0 {
			sfrm.MailFileNamePattern_Suffixes = make([]string, len(sfrm.MailFileNamePattern_Prefixes))
		}

		for i := 0; i < len(sups); i++ {
			if sups[i].Name == sfrm.Name {
				return nil, errors.New("supplier's config duplicates: " + sfrm.Name)
			}
		}
		sups = append(sups, sfrm)
	}
	return sups, nil
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

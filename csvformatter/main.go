package main

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
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
	RawCsvPath   string
	CsvPath      string
	TimerSeconds int
	OutFormat    []int // delim(rune), brand col, articul col, name col, partnum col, price col, quantity col
}

type suppliers struct {
	Iversa    *format
	Unicom    *format
	Berg      *format
	Voskhod   *format
	Tungusov  *format
	Torrex    *format
	Partcom   *format
	Avanta    *format
	Forumauto *format
	Rossco    *format
	Shate     *format
	Planeta   *format
	Armtek    *format
	VostokCHB *format
}

type format struct {
	Name        string
	Mail        string
	Outfilename string
	Delim       string
	Quotes      int
	Firstrow    int

	Brandcol    int
	Artcol      int
	Namecol     []int
	Partnumcol  int
	Pricecol    int
	Quantitycol int
}

func main() {
	conf := &config{}
	sups := &suppliers{}
	pfd, err := confdecoder.ParseFile("config.txt")
	if err != nil {
		panic("read config file err: " + err.Error())
	}
	pfd.NestedStructsMode = confdecoder.NestedStructsModeTwo
	err = pfd.DecodeTo(conf, sups)
	if err != nil {
		panic("decode config file err: " + err.Error())
	}
	if conf.RawCsvPath == "" {
		panic("no RawCsvPath specified in config.txt")
	}
	if conf.CsvPath == "" {
		panic("no CsvPath specified in config.txt")
	}
	if conf.OutFormat == nil || len(conf.OutFormat) != 7 {
		panic("no OutFormat specified or num of values dont match (must be 7 values)")
	}
	if conf.TimerSeconds == 0 {
		panic("no TimerSeconds specified in config.txt or is zero")
	}
	conf.RawCsvPath += "/"
	conf.CsvPath += "/"

	ctx, cancel := createContextWithInterruptSignal()

	flsh := logger.NewFlusher(encode.DebugLevel)
	l := flsh.NewLogsContainer("csvformatter")

	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(conf.TimerSeconds))
		l.Debug("Job", "started")
		conf.do_job(l, cancel, sups)
		l.Debug("Job", "done, sleeping")
		for {
			select {
			case <-ctx.Done():
				l.Info("Routine", "context done, exiting loop")
				return
			case <-ticker.C:
				l.Debug("Job", "started")
				conf.do_job(l, cancel, sups)
				l.Debug("Job", "done, sleeping")
			}

		}
	}()

	<-ctx.Done()
	l.Debug("Context", "done, exiting")
	flsh.Close()
	flsh.DoneWithTimeout(time.Second * 5)
}

func (c *config) do_job(l logger.Logger, cancel context.CancelFunc, sups *suppliers) {
	l.Debug("Format", "started")
	files, err := os.ReadDir(c.RawCsvPath)
	if err != nil {
		l.Error("Format/ReadDir", err)
		cancel()
		return
	}
	var frmt *format

	for _, f := range files {
		fname_lowered := strings.ToLower(f.Name())
		if f.IsDir() || !strings.HasSuffix(fname_lowered, ".csv") {
			l.Warning("Format/ReadDir", "noncsv file founded "+f.Name())
			continue
		}
		if strings.HasPrefix(fname_lowered, "price4autodocplus_ekat") {
			// if err := encode(p); err != nil {  // TODO: ???
			// 	log.Panicln(err)
			// }
			// err = os.Rename(p, "price10.csv")
			frmt = sups.Partcom

			goto found
		}
		if strings.HasPrefix(fname_lowered, "export_ekaterinburg") {
			// if err := encode(p); err != nil {  // TODO: ???
			// 	log.Panicln(err)
			// }
			// err = os.Rename(p, "price8.csv")
			frmt = sups.Shate
			goto found
		}
		if strings.HasPrefix(fname_lowered, "berg_") {
			// err = os.Rename(p, "price4.csv")
			frmt = sups.Berg
			goto found
		}
		if strings.HasPrefix(fname_lowered, "прайс лист для клиентов") {
			// err = os.Rename(p, "price15.csv")
			// if err = c.formatCSV(f.Name(), sups.); err != nil {
			// 	l.Error("Format/formatcsv", err)
			// }
			l.Error("Format", errors.New("i dunno what supplier's csv it is: "+f.Name()))
			goto found
		} else if strings.HasPrefix(fname_lowered, "прайс") {
			// err = os.Rename(p, "price6.csv")
			frmt = sups.Tungusov
			goto found
		}
		if strings.HasPrefix(fname_lowered, "forum_") {
			// err = os.Rename(p, "price9.csv")
			frmt = sups.Forumauto
			goto found
		}
		if strings.HasPrefix(fname_lowered, "price_for_") {
			// err = os.Rename(p, "price13.csv")
			frmt = sups.Avanta
			goto found
		}
		if strings.HasPrefix(fname_lowered, "price_list") {
			// err = os.Rename(p, "price.csv")
			frmt = sups.Planeta
			goto found
		}
		if strings.HasPrefix(fname_lowered, "8456") {
			// err = os.Rename(p, "price11.csv")
			frmt = sups.Rossco
			goto found
		}
		if strings.HasPrefix(fname_lowered, "автодок.") {
			// err = os.Rename(p, "price3.csv")
			frmt = sups.Unicom
			goto found
		}
		if strings.HasPrefix(fname_lowered, "обществосограниченнойответственностью") {
			if strings.HasSuffix(fname_lowered, "чб.csv") {
				// err = os.Rename(p, "price14.csv")
				frmt = sups.VostokCHB
				goto found
			} else {
				// err = os.Rename(p, "price5.csv")
				frmt = sups.Voskhod
				goto found
			}
		}
		if strings.HasPrefix(fname_lowered, "pricetiss") {
			// err = os.Rename(p, "price7.csv")
			frmt = sups.Torrex
			goto found
		}
		if strings.HasPrefix(f.Name(), "╨Х╨║╨▒") {
			// err = os.Rename(p, "price12.csv")
			frmt = sups.Armtek
		}

		l.Error("Format", errors.New("unknown rawcsv filename: "+f.Name()))
		continue
	found:
		if err = c.formatCSV(f.Name(), frmt); err != nil {
			l.Error("Format/formatCSV", err)
			continue
		}
		l.Debug("Format", "csv formatted: "+f.Name()+" to: "+frmt.Outfilename)

		// if err = os.Remove(c.RawCsvPath + f.Name()); err != nil {
		// 	l.Error("Format/Remove", err)
		// }
		// l.Debug("Format", "removed "+f.Name())
	}
	l.Debug("Format", "done")

}

// нет проверки соответствия форматов длинам слайсов
// does NOT lock dir
func (c *config) formatCSV(filename string, frmt *format) error {
	time.Sleep(time.Second)
	if frmt == nil || frmt.Name == "" || frmt.Outfilename == "" {
		return errors.New("nil or empty given format")
	}
	if filename == "" {
		return errors.New("empty given filename")
	}
	rawfile, err := os.Open(c.RawCsvPath + filename)
	if err != nil {
		return err
	}
	defer rawfile.Close()
	cleanfile, err := os.Create(c.CsvPath + frmt.Outfilename)
	if err != nil {
		return err
	}
	defer cleanfile.Close()

	r := csv.NewReader(rawfile)
	r.Comma = rune(frmt.Delim[0])
	r.LazyQuotes = frmt.Quotes == 1

	w := csv.NewWriter(cleanfile)
	w.Comma = rune(c.OutFormat[0])
	w.UseCRLF = true
	buf := make([]string, 6)
	buf[c.OutFormat[1]], buf[c.OutFormat[2]], buf[c.OutFormat[3]], buf[c.OutFormat[4]], buf[c.OutFormat[5]], buf[c.OutFormat[6]] = "BRAND", "ARTICUL", "NAME", "PARTNUM", "PRICE", "QUANTITY"
	err = w.Write(buf)
	if err != nil {
		return err
	}
	if frmt.Firstrow > 0 {
		for i := 0; i < frmt.Firstrow; i++ {
			_, err = r.Read()
			if err != nil {
				return err
			}
		}
	}
	for {
		readed, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		var partnum string
		if frmt.Partnumcol > 0 {
			partnum = readed[frmt.Partnumcol]
		}
		name := strings.TrimSpace(readed[frmt.Namecol[0]])
		if len(frmt.Namecol) > 1 {
			for i := 1; i < len(frmt.Namecol); i++ {
				name += " " + strings.TrimSpace(readed[frmt.Namecol[i]])
			}
		}
		buf[c.OutFormat[1]], buf[c.OutFormat[2]], buf[c.OutFormat[3]], buf[c.OutFormat[4]], buf[c.OutFormat[5]], buf[c.OutFormat[6]] = readed[frmt.Brandcol], readed[frmt.Artcol], name, partnum, readed[frmt.Pricecol], readed[frmt.Quantitycol]
		err = w.Write(buf)
		if err != nil {
			return err
		}
	}
	return nil
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

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
	RawCsvPath                 string
	CsvPath                    string
	TimerSeconds               int
	SuppliersCsvFormatFilePath string
	SuppliersCsvFormat         *format
}

type format struct {
	Delimeter   string
	Quotes      int
	FirstRow    int
	BrandCol    int
	ArticulCol  int
	NameCol     int
	PartnumCol  int
	PriceCol    int
	QuantityCol int
	RestCol     int
}

func main() {
	conf := &config{}
	err := confdecoder.DecodeFile("config.txt", conf)
	if err != nil {
		panic("read config file err: " + err.Error())
	}
	if conf.RawCsvPath == "" {
		panic("no RawCsvPath specified in config.txt")
	}
	if conf.CsvPath == "" {
		panic("no CsvPath specified in config.txt")
	}
	if conf.TimerSeconds == 0 {
		panic("no TimerSeconds specified in config.txt or is zero")
	}
	err = confdecoder.DecodeFile(conf.SuppliersCsvFormatFilePath, conf.SuppliersCsvFormat)
	if err != nil {
		panic("read SuppliersCsvFormatFile file err: " + err.Error())
	}
	if conf.SuppliersCsvFormat.Delimeter == "" {
		panic("bad SuppliersCsvFormat")
	}
	if conf.SuppliersCsvFormatFilePath == "" {
		panic("no SuppliersCsvFormatFilePath specified or num of values dont match (must be 7 values)")
	}

	rep, err := OpenRepository("postgres://ozon:q13471347@localhost:5432/ozondb")
	if err != nil {
		panic(err)
	}
	defer rep.db.Close(context.Background())

	conf.RawCsvPath += "/"
	conf.CsvPath += "/"

	ctx, cancel := createContextWithInterruptSignal()

	flsh := logger.NewFlusher(encode.DebugLevel)
	l := flsh.NewLogsContainer("csvformatter")

	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(conf.TimerSeconds))
		l.Debug("Job", "started")
		sups, err := rep.GetSuppliersByNames()
		if err != nil {
			l.Error("GetSuppliersByNames", err)
			l.Error("Job", errors.New("cant do without suppliers"))
		} else {
			conf.do_job(l, cancel, sups)
			l.Debug("Job", "done, sleeping")
		}
		for {
			select {
			case <-ctx.Done():
				l.Info("Routine", "context done, exiting loop")
				return
			case <-ticker.C:
				l.Debug("Job", "started")
				sups, err = rep.GetSuppliersByNames()
				if err != nil {
					l.Error("GetSuppliersByNames", err)
					l.Error("Job", errors.New("cant do without suppliers"))
				} else {
					conf.do_job(l, cancel, sups)
					l.Debug("Job", "done, sleeping")
				}
			}

		}
	}()

	<-ctx.Done()
	l.Debug("Context", "done, exiting")
	flsh.Close()
	flsh.DoneWithTimeout(time.Second * 5)
}

func (c *config) do_job(l logger.Logger, cancel context.CancelFunc, sups map[string]supplier) {
	l.Debug("Format", "started")
	files, err := os.ReadDir(c.RawCsvPath)
	if err != nil {
		l.Error("Format/ReadDir", err)
		cancel()
		return
	}

	for _, f := range files {
		var sup supplier
		var ername string
		var ok bool
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
			if sup, ok = sups["Партком"]; !ok {
				ername = "Партком"
			}

			goto found
		}
		if strings.HasPrefix(fname_lowered, "export_ekaterinburg") {
			// if err := encode(p); err != nil {  // TODO: ???
			// 	log.Panicln(err)
			// }
			// err = os.Rename(p, "price8.csv")
			if sup, ok = sups["Shate"]; !ok {
				ername = "Shate"
			}
			goto found
		}
		if strings.HasPrefix(fname_lowered, "berg_") {
			// err = os.Rename(p, "price4.csv")
			if sup, ok = sups["БЕРГ"]; !ok {
				ername = "БЕРГ"
			}
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
			if sup, ok = sups["Тунгусов"]; !ok {
				ername = "Тунгусов"
			}
			goto found
		}
		if strings.HasPrefix(fname_lowered, "forum_") {
			// err = os.Rename(p, "price9.csv")
			if sup, ok = sups["Forum-Auto"]; !ok {
				ername = "Forum-Auto"
			}
			goto found
		}
		if strings.HasPrefix(fname_lowered, "price_for_") {
			// err = os.Rename(p, "price13.csv")
			if sup, ok = sups["Avanta"]; !ok {
				ername = "Avanta"
			}
			goto found
		}
		if strings.HasPrefix(fname_lowered, "price_list") {
			// err = os.Rename(p, "price.csv")
			if sup, ok = sups["Планета"]; !ok {
				ername = "Планета"
			}
			goto found
		}
		if strings.HasPrefix(fname_lowered, "8456") {
			// err = os.Rename(p, "price11.csv")
			if sup, ok = sups["Росско"]; !ok {
				ername = "Росско"
			}
			goto found
		}
		if strings.HasPrefix(fname_lowered, "автодок.") {
			// err = os.Rename(p, "price3.csv")
			if sup, ok = sups["Юником"]; !ok {
				ername = "Юником"
			}
			goto found
		}
		if strings.HasPrefix(fname_lowered, "обществосограниченнойответственностью") {
			if strings.HasSuffix(fname_lowered, "чб.csv") {
				// err = os.Rename(p, "price14.csv")
				if sup, ok = sups["Восток ЧБ"]; !ok {
					ername = "Восток ЧБ"
				}
				goto found
			} else {
				// err = os.Rename(p, "price5.csv")
				if sup, ok = sups["Восход"]; !ok {
					ername = "Восход"
				}
				goto found
			}
		}
		if strings.HasPrefix(fname_lowered, "pricetiss") {
			// err = os.Rename(p, "price7.csv")
			if sup, ok = sups["Торрекс"]; !ok {
				ername = "Торрекс"
			}
			goto found
		}
		if strings.HasPrefix(f.Name(), "╨Х╨║╨▒") {
			// err = os.Rename(p, "price12.csv")
			if sup, ok = sups["Армтек"]; !ok {
				ername = "Армтек"
			}
			goto found
		}
		if ername != "" {
			l.Error("Format", errors.New("cant find supplier with name: "+ername))
			continue
		}
		l.Error("Format", errors.New("unknown rawcsv filename: "+f.Name()))
		continue
	found:
		if err = c.formatCSV(f.Name(), sup); err != nil {
			l.Error("Format/formatCSV", err)
			continue
		}
		l.Debug("Format", "csv formatted: "+f.Name()+" to: "+sup.Filename)

		// if err = os.Remove(c.RawCsvPath + f.Name()); err != nil {
		// 	l.Error("Format/Remove", err)
		// }
		// l.Debug("Format", "removed "+f.Name())
	}
	l.Debug("Format", "done")

}

// нет проверки соответствия форматов длинам слайсов
// does NOT lock dir
func (c *config) formatCSV(filename string, sup supplier) error {
	if sup.Filename == "" {
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
	cleanfile, err := os.Create(c.CsvPath + sup.Filename)
	if err != nil {
		return err
	}
	defer cleanfile.Close()

	r := csv.NewReader(rawfile)
	r.Comma = rune(sup.Delimiter[0])
	r.LazyQuotes = sup.Quotes == 1

	w := csv.NewWriter(cleanfile)
	w.Comma = rune(c.SuppliersCsvFormat.Delimeter[0])
	w.UseCRLF = c.SuppliersCsvFormat.Quotes == 1
	buf := make([]string, 8)
	buf[c.SuppliersCsvFormat.BrandCol], buf[c.SuppliersCsvFormat.ArticulCol], buf[c.SuppliersCsvFormat.NameCol], buf[c.SuppliersCsvFormat.PartnumCol], buf[c.SuppliersCsvFormat.PriceCol], buf[c.SuppliersCsvFormat.QuantityCol], buf[c.SuppliersCsvFormat.RestCol] = "BRAND", "ARTICUL", "NAME", "PARTNUM", "PRICE", "QUANTITY", "REST"
	err = w.Write(buf)
	if err != nil {
		return err
	}
	if sup.FirstRow > 0 {
		for i := 0; i < sup.FirstRow; i++ {
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
		if sup.PartnumCol > 0 {
			partnum = readed[sup.PartnumCol]
		}
		name := strings.TrimSpace(readed[sup.NameCol[0]])
		if len(sup.NameCol) > 1 {
			for i := 1; i < len(sup.NameCol); i++ {
				name += " " + strings.TrimSpace(readed[sup.NameCol[i]])
			}
		}
		buf[c.SuppliersCsvFormat.BrandCol],
			buf[c.SuppliersCsvFormat.ArticulCol],
			buf[c.SuppliersCsvFormat.NameCol],
			buf[c.SuppliersCsvFormat.PartnumCol],
			buf[c.SuppliersCsvFormat.PriceCol],
			buf[c.SuppliersCsvFormat.QuantityCol],
			buf[c.SuppliersCsvFormat.RestCol] = readed[sup.BrandCol], readed[sup.ArticulCol], name, partnum, readed[sup.PriceCol], readed[sup.QuantityCol], strings.Trim(readed[sup.RestCol], "~<>")
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

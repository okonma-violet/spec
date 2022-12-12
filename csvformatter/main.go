package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"

	"strings"
	"syscall"
	"time"

	"github.com/okonma-violet/confdecoder"
	"github.com/okonma-violet/spec/logs/encode"
	"github.com/okonma-violet/spec/logs/logger"
	"golang.org/x/text/encoding/charmap"
)

type config struct {
	RawCsvPath   string
	CsvPath      string
	TimerSeconds int

	SuppliersConfsPath         string
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
	if conf.SuppliersConfsPath == "" {
		panic("no CsvPath specified in config.txt")
	}
	if conf.TimerSeconds == 0 {
		panic("no TimerSeconds specified in config.txt or is zero")
	}
	if conf.SuppliersCsvFormatFilePath == "" {
		panic("no SuppliersCsvFormatFilePath specified  in config.txt")
	}

	err = confdecoder.DecodeFile(conf.SuppliersCsvFormatFilePath, conf.SuppliersCsvFormat)
	if err != nil {
		panic("read SuppliersCsvFormatFile file err: " + err.Error())
	}
	if conf.SuppliersCsvFormat.Delimeter == "" {
		panic("bad readed SuppliersCsvFormat")
	}

	conf.SuppliersConfsPath += "/"

	conf.RawCsvPath += "/"
	conf.CsvPath += "/"

	ctx, cancel := createContextWithInterruptSignal()

	flsh := logger.NewFlusher(encode.DebugLevel)
	l := flsh.NewLogsContainer("csvformatter")

	go func() {
		//ticker := time.NewTicker(time.Second * time.Duration(conf.TimerSeconds))
		l.Debug("Job", "started")
		sups, err := LoadSuppliers(conf.SuppliersConfsPath)
		if err != nil {
			l.Error("LoadSuppliers", err)
			cancel()
			return
		}

		conf.do_job(l, cancel, sups)

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
		var ok, encode_needed bool
		fname_lowered := strings.ToLower(f.Name())
		if f.IsDir() || !strings.HasSuffix(fname_lowered, ".csv") {
			l.Warning("Format/ReadDir", "noncsv file founded "+f.Name())
			continue
		}
		if strings.HasPrefix(fname_lowered, "price4autodocplus_ekat") {
			ername, encode_needed = "Партком", true
			if sup, ok = sups[ername]; ok {
				goto found
			}
			goto notfound
		}
		if strings.HasPrefix(fname_lowered, "price_ekb2") {
			ername = "Иверса"
			if sup, ok = sups[ername]; ok {
				goto found
			}
			goto notfound
		}
		if strings.HasPrefix(fname_lowered, "export_ekaterinburg") {
			ername, encode_needed = "Shate", true
			if sup, ok = sups[ername]; ok {
				goto found
			}
			goto notfound
		}
		if strings.HasPrefix(fname_lowered, "berg_") {
			ername = "БЕРГ"
			if sup, ok = sups[ername]; ok {
				goto found
			}
			goto notfound
		}
		if strings.HasPrefix(fname_lowered, "прайс лист для клиентов") {
			ername = "Avanta"
			if sup, ok = sups[ername]; ok {
				goto found
			}
			goto notfound
		} else if strings.HasPrefix(fname_lowered, "прайс") {
			ername = "Тунгусов"
			if sup, ok = sups[ername]; ok {
				goto found
			}
			goto notfound
		}
		if strings.HasPrefix(fname_lowered, "forum_") {
			ername = "Forum-Auto"
			if sup, ok = sups[ername]; ok {
				goto found
			}
			goto notfound
		}
		if strings.HasPrefix(fname_lowered, "price_for_") {
			ername = "Avanta"
			if sup, ok = sups[ername]; ok {
				goto found
			}
			goto notfound
		}
		if strings.HasPrefix(fname_lowered, "price_list") {
			ername = "Планета"
			if sup, ok = sups[ername]; ok {
				goto found
			}
			goto notfound
		}
		if strings.HasPrefix(fname_lowered, "8456") {
			ername = "Росско"
			if sup, ok = sups[ername]; ok {
				goto found
			}
			goto notfound
		}
		if strings.HasPrefix(fname_lowered, "автодок.") {
			ername = "Юником"
			if sup, ok = sups[ername]; ok {
				goto found
			}
			goto notfound
		}
		if strings.HasPrefix(fname_lowered, "обществосограниченнойответственностью") {
			if strings.HasSuffix(fname_lowered, "чб.csv") {
				ername = "Восток ЧБ"
				if sup, ok = sups[ername]; ok {
					goto found
				}
				goto notfound
			} else {
				ername = "Восход"
				if sup, ok = sups[ername]; ok {
					goto found
				}
				goto notfound
			}
		}
		if strings.HasPrefix(fname_lowered, "pricetiss") {
			ername = "Торекс"
			if sup, ok = sups[ername]; ok {
				goto found
			}
			goto notfound
		}
		if strings.HasPrefix(f.Name(), "╨Я╤А╨") {
			ername = "Армтек"
			if sup, ok = sups[ername]; ok {
				goto found
			}
			goto notfound
		}
	notfound:
		if ername != "" {
			l.Error("Format", errors.New("cant find supplier with name: "+ername))
			continue
		}
		l.Error("Format", errors.New("unknown rawcsv filename: "+f.Name()))
		continue
	found:
		if err = c.formatCSV(f.Name(), sup, encode_needed); err != nil {
			l.Error("Format/formatCSV", errors.New("file: "+f.Name()+", err: "+err.Error()))
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
func (c *config) formatCSV(filename string, sup supplier, encode_needed bool) error {
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

	var def_r io.Reader = rawfile
	if encode_needed {
		def_r = charmap.Windows1251.NewDecoder().Reader(def_r)
	}
	r := csv.NewReader(def_r)
	r.Comma = []rune(sup.Delimiter)[0]
	r.LazyQuotes = sup.Quotes == 1

	w := csv.NewWriter(cleanfile)
	w.Comma = []rune(c.SuppliersCsvFormat.Delimeter)[0]
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
		var quantity string
		if sup.QuantityCol >= 0 {
			quantity = readed[sup.QuantityCol]
		} else {
			quantity = "0"
		}
		buf[c.SuppliersCsvFormat.BrandCol],
			buf[c.SuppliersCsvFormat.ArticulCol],
			buf[c.SuppliersCsvFormat.NameCol],
			buf[c.SuppliersCsvFormat.PartnumCol],
			buf[c.SuppliersCsvFormat.PriceCol],
			buf[c.SuppliersCsvFormat.QuantityCol],
			buf[c.SuppliersCsvFormat.RestCol] = strings.TrimSpace(readed[sup.BrandCol]), normart(readed[sup.ArticulCol]), normnaim(name), partnum, normprice(readed[sup.PriceCol]), quantity, normnum(readed[sup.RestCol])
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

type supplier struct {
	Name        string
	Email       string
	Filename    string
	Delimiter   string
	Quotes      int
	FirstRow    int
	BrandCol    int
	ArticulCol  int
	NameCol     []int
	PartnumCol  int
	PriceCol    int
	QuantityCol int
	RestCol     int
}

func LoadSuppliers(path string) (map[string]supplier, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	sups := make(map[string]supplier)

	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".txt") {
			fmt.Println("LoadSuppliers", "nontxt founded:", f.Name())
			continue
		}

		sfrm := supplier{}
		err := confdecoder.DecodeFile(path+f.Name(), &sfrm)
		if err != nil {
			return nil, errors.New("read supplier's config file err: " + err.Error())
		}
		if sfrm.Name == "" || sfrm.Filename == "" {
			fmt.Println("LoadSuppliers", "no data in supplier's config file:", f.Name())
			continue
		}

		if _, ok := sups[sfrm.Name]; !ok {
			sups[sfrm.Name] = sfrm
		} else {
			fmt.Println("LoadSuppliers", "supplier's config duplicates:", sfrm.Name)
		}
	}
	return sups, nil
}

var pricerx = regexp.MustCompile("[^а-яa-z0-9.,]")
var naimrx = regexp.MustCompile(`\s{2,}`)
var artrx = regexp.MustCompile("[^а-яa-z0-9]")
var numberrx = regexp.MustCompile(`[^0-9]`)

func normprice(s string) string {
	return pricerx.ReplaceAllString(s, "")
}
func normnum(s string) string {
	return numberrx.ReplaceAllString(strings.ToLower(s), "")
}
func normnaim(s string) string {
	return naimrx.ReplaceAllString(strings.TrimSpace(s), " ")
}

func normart(s string) string {
	return artrx.ReplaceAllString(strings.ToLower(s), "")
}

package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"

	"io"
	"os"
	"os/signal"
	"regexp"
	"sort"

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

	rp := flag.Bool("r", false, "remove processed csv files")
	flag.Parse()

	conf.SuppliersConfsPath += "/"

	conf.RawCsvPath += "/"
	conf.CsvPath += "/"

	ctx, cancel := createContextWithInterruptSignal()

	flsh := logger.NewFlusher(encode.DebugLevel)
	l := flsh.NewLogsContainer("csvformatter")

	go func() {
		//ticker := time.NewTicker(time.Second * time.Duration(conf.TimerSeconds))
		l.Debug("Job", "started")
		sups, err := loadSuppliersConfigsFromDir(l, conf.SuppliersConfsPath)
		if err != nil {
			l.Error("LoadSuppliers", err)
			cancel()
			return
		}
		sort.Sort(sups)

		conf.do_job(l, cancel, *rp, sups)

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

func (c *config) do_job(l logger.Logger, cancel context.CancelFunc, remove_processed bool, sups supplierslist) {
	l.Debug("Format", "started")
	files, err := os.ReadDir(c.RawCsvPath)
	if err != nil {
		l.Error("Format/ReadDir", err)
		cancel()
		return
	}
loop:
	for _, f := range files {
		fname_lowered := strings.ToLower(f.Name())
		if f.IsDir() || !strings.HasSuffix(fname_lowered, ".csv") {
			l.Warning("Format/ReadDir", "noncsv file founded "+f.Name())
			continue
		}
		for i := 0; i < len(sups); i++ {
			if strings.HasPrefix(fname_lowered, sups[i].RawCsvNamePattern_Prefix) {
				if sups[i].RawCsvNamePattern_Suffix != "" && !strings.HasSuffix(fname_lowered, sups[i].RawCsvNamePattern_Suffix) {
					continue
				}
				if err = c.formatCSV(f.Name(), sups[i]); err != nil {
					l.Error("Format/formatCSV", errors.New("file: "+f.Name()+", err: "+err.Error()))
					continue
				}
				l.Debug("Format", "csv formatted: "+f.Name()+" to: "+sups[i].Filename)

				if remove_processed {
					if err = os.Remove(c.RawCsvPath + f.Name()); err != nil {
						l.Error("Format/Remove", err)
					}
					l.Debug("Format", "removed "+f.Name())
				}
				continue loop

			}
		}
		l.Error("Format", errors.New("unknown rawcsv filename: "+f.Name()))
	}
	l.Debug("Format", "done")
}

// нет проверки соответствия форматов длинам слайсов
// does NOT lock dir
func (c *config) formatCSV(filename string, sup *supplier) error {
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
	if sup.Charset != "" {
		if sup.Charset == "1251" {
			def_r = charmap.Windows1251.NewDecoder().Reader(def_r)
		} else {
			return errors.New("unsupportable charset: " + sup.Charset)
		}
	}
	r := csv.NewReader(def_r)
	r.Comma = []rune(sup.Delimiter)[0]
	r.LazyQuotes = sup.Quotes == 1
	r.ReuseRecord = true

	w := csv.NewWriter(cleanfile)
	w.Comma = []rune(c.SuppliersCsvFormat.Delimeter)[0]
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
	w.Flush()
	return nil
}

type supplierslist []*supplier

func (sl supplierslist) Len() int {
	return len(sl)
}
func (sl supplierslist) Less(i, j int) bool {
	if sl[i].RawCsvNamePattern_Prefix == sl[j].RawCsvNamePattern_Prefix && sl[i].RawCsvNamePattern_Suffix == sl[j].RawCsvNamePattern_Suffix {
		panic("equal prefix & suffix in sups: " + sl[i].Name + ", " + sl[j].Name)
	}
	return len(sl[i].RawCsvNamePattern_Prefix)+len(sl[i].RawCsvNamePattern_Suffix) > len(sl[j].RawCsvNamePattern_Prefix)+len(sl[j].RawCsvNamePattern_Suffix)
}
func (sl supplierslist) Swap(i, j int) {
	b := sl[i]
	sl[i] = sl[j]
	sl[j] = b
}

type supplier struct {
	Name                     string
	Email                    string
	Filename                 string
	Delimiter                string
	Quotes                   int
	FirstRow                 int
	BrandCol                 int
	ArticulCol               int
	NameCol                  []int
	PartnumCol               int
	PriceCol                 int
	QuantityCol              int
	RestCol                  int
	RawCsvNamePattern_Prefix string
	RawCsvNamePattern_Suffix string
	Charset                  string
}

func loadSuppliersConfigsFromDir(l logger.Logger, path string) (supplierslist, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	sups := make([]*supplier, 0, len(files))

	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".txt") {
			l.Warning("LoadSuppliers", "nontxt & nonzip founded: "+f.Name())
			continue
		}

		sfrm := supplier{}
		err := confdecoder.DecodeFile(path+f.Name(), &sfrm)
		sfrm.RawCsvNamePattern_Prefix = strings.ToLower(sfrm.RawCsvNamePattern_Prefix)
		sfrm.RawCsvNamePattern_Suffix = strings.ToLower(sfrm.RawCsvNamePattern_Suffix)
		if err != nil {
			return nil, errors.New("read supplier's config file err: " + err.Error())
		}
		if sfrm.Name == "" || sfrm.Filename == "" {
			l.Error("LoadSuppliers", errors.New("no data in supplier's config file: "+f.Name()))
			continue
		}
		if sfrm.RawCsvNamePattern_Prefix == "" && sfrm.RawCsvNamePattern_Suffix == "" {
			l.Error("LoadSuppliers", errors.New("no prefix and no suffix in supplier's config file: "+f.Name()))
			continue
		}

		for i := 0; i < len(sups); i++ {
			if sups[i].Name == sfrm.Name || sups[i].Filename == sfrm.Filename {
				return nil, errors.New("supplier's config duplicates: " + sfrm.Name)
			}
		}
		sups = append(sups, &sfrm)
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

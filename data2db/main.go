package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"time"

	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/okonma-violet/confdecoder"
	"github.com/okonma-violet/spec/locker"
	"github.com/okonma-violet/spec/logs/encode"
	"github.com/okonma-violet/spec/logs/logger"
)

type config struct {
	ProductsCsvPath             string
	SuppliersConfsPath          string
	BrandsFilePath              string
	AlternativeArticulsFilePath string
	SuppliersCsvFormatFilePath  string
	CategoriesFilePath          string

	TimerSeconds int

	SuppliersCsvFormat *format
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

const waitdirlock_time = time.Second * 5
const maxwaittimes = 3

var needunlock bool

// DROPS AND RECREATES ALL TABLES ON MIGRATION !!!!!!!!!!!!!!
// TRUNCATES CATEGORY'S KEYWORD'S FILE EVERY LAUNCH !!!!!!!!!!!!!!
func main() {
	conf := &config{}
	err := confdecoder.DecodeFile("config.txt", conf)
	if err != nil {
		panic("read config file err: " + err.Error())
	}
	if conf.ProductsCsvPath == "" || conf.AlternativeArticulsFilePath == "" || conf.CategoriesFilePath == "" || conf.SuppliersConfsPath == "" || conf.SuppliersCsvFormatFilePath == "" || conf.BrandsFilePath == "" {
		panic("ProductsCsvPath or AlternativeArticulsFilePath or SuppliersCsvFormatPath or SuppliersConfsPath or BrandsFilePath not specified in config.txt")
	}
	err = confdecoder.DecodeFile(conf.SuppliersCsvFormatFilePath, conf.SuppliersCsvFormat)
	if err != nil {
		panic("read config file err: " + err.Error())
	}
	if conf.SuppliersCsvFormat.Delimeter == "" {
		panic("bad SuppliersCsvFormat")
	}
	mgrt := flag.Bool("m", false, "migrate tables")
	drp := flag.Bool("d", false, "drop tables if exists")
	lb := flag.Bool("b", false, "load brands from csv")
	ls := flag.Bool("s", false, "load sups configs")
	lc := flag.Bool("c", false, "load categories from csv")
	upl := flag.Bool("u", false, "upload products from csvs")
	ctgrz := flag.Bool("C", false, "categorize")
	rp := flag.Bool("r", false, "remove processed csv files")
	flag.Parse()

	ctx, cancel := createContextWithInterruptSignal(&needunlock, conf.ProductsCsvPath)

	flsh := logger.NewFlusher(encode.DebugLevel)
	l := flsh.NewLogsContainer("data2db")
	if *rp {
		l.Info("Flag", "removing processed files enabled")
	}
	rep := &repo{}
	err = rep.OpenDBRepository("postgres://ozon:q13471347@localhost:5432/ozondb")
	if err != nil {
		panic(err)
	}
	defer rep.db.Close(context.Background())
	conf.ProductsCsvPath += "/"
	conf.SuppliersConfsPath += "/"

	if *mgrt {
		if *drp {
			l.Debug("Init", "Creating tables, drop if exists enabled")
		} else {
			l.Debug("Init", "Creating tables, drop if exists disabled")
		}
		if err = rep.Migrate(*drp); err != nil {
			panic(err)
		}
	}

	// CREATING BRANDS
	if *lb {
		l.Debug("Init", "Load brands from file")
		if err := rep.loadBrandsFromFile(conf.BrandsFilePath); err != nil {
			panic(err)
		}
	}

	// CREATING SUPPLIERS
	if *ls {
		l.Debug("Init", "Load suppliers configs")
		if err = rep.loadSuppliersConfigsFromDir(conf.SuppliersConfsPath); err != nil {
			panic(err)
		}
	}

	// CREATING CATEGORIES WITH KEYPHRASES
	if *lc {
		l.Debug("Init", "Load categories with keyphrases")
		if err = rep.loadCategoriesWithKeyphrasesFromFile(conf.CategoriesFilePath); err != nil {
			panic(err)
		}
	}

	// UPLOAD

	if *upl {
		go func() {
			l.Info("Upload Routine", "loop started")
			ticker := time.NewTicker(time.Second * time.Duration(conf.TimerSeconds))
			l.Debug("Job", "started")

			if err = rep.db.Ping(ctx); err != nil {
				l.Error("DB.Ping", err)
				l.Debug("DB", "reconnecting")
				if err = rep.OpenDBRepository("postgres://ozon:q13471347@localhost:5432/ozondb"); err != nil {
					l.Error("OpenDBRepository", err)
					l.Debug("Job", "cant work without db connection, sleeping")
				} else {
					conf.upload(l, rep, *rp)
					l.Debug("Job", "done")
				}
			} else {
				conf.upload(l, rep, *rp)
				l.Debug("Job", "done")
			}

			for {
				select {
				case <-ctx.Done():
					l.Info("Upload Routine", "context done, exiting loop")
					return
				case <-ticker.C:
					l.Debug("Job", "started")
					if err = rep.db.Ping(ctx); err != nil {
						l.Error("DB.Ping", err)
						l.Debug("DB", "reconnecting")
						if err = rep.OpenDBRepository("postgres://ozon:q13471347@localhost:5432/ozondb"); err != nil {
							l.Error("OpenDBRepository", err)
							l.Debug("Job", "cant work without db connection, sleeping")
							continue
						}
					}
					conf.upload(l, rep, *rp)
					l.Debug("Job", "done, sleeping")
				}
			}
		}()
	}

	// CATEGORIZE UNCATEGORIZED
	if *ctgrz {
		if err = rep.Categorize(); err != nil {
			fmt.Println("Categorize", err)
		}
	}

	if !*upl {
		cancel()
	}
	<-ctx.Done()
	l.Debug("Context", "done, exiting")
	flsh.Close()
	flsh.DoneWithTimeout(time.Second * 5)
}

func (conf *config) upload(l logger.Logger, rep *repo, remove_processed bool) {
	var lckd bool
	for i := 0; i < maxwaittimes; i++ {
		if err := locker.LockDir(conf.ProductsCsvPath); err != nil {
			if errors.Is(err, locker.ErrLocked) {
				l.Error("LockDir", errors.New("rawcsv dir locked"))
				time.Sleep(waitdirlock_time)
			} else {
				l.Error("LockDir", err)
				return
			}
		} else {
			lckd = true
			break
		}
	}
	if !lckd {
		l.Error("LockDir", errors.New("tries over, returning"))
		return
	}
	needunlock = true
	defer func() {
		locker.UnlockDir(conf.ProductsCsvPath)
		needunlock = false
	}()

	files, err := os.ReadDir(conf.ProductsCsvPath)
	if err != nil {
		l.Error("ReadDir", err)
		return
	}
	if len(files) == 1 && files[0].Name() == locker.LockfileName {
		l.Debug("ReadDir", "no files")
		return
	}

	// CREATING ALTERNATIVE ARTICULES
	altarts, err := loadAlternativeArticulesFromFile(conf.AlternativeArticulsFilePath)
	if err != nil {
		l.Error("loadAlternativeArticulesFromFile", err)
		return
	}

	// CREATE UPLOAD
	uploadid, err := rep.CreateUpload()
	if err != nil {
		l.Error("CreateUpload", err)
		return
	}

	// FILES LOOP
fileloop:
	for _, f := range files {

		l.Debug("Reading file", f.Name())
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".csv") {
			if f.Name() == locker.LockfileName {
				continue
			}
			l.Warning("Format/ReadDir", "noncsv file founded "+f.Name())
			continue
		}
		file, err := os.Open(conf.ProductsCsvPath + f.Name())
		if err != nil {
			l.Error("os.Open", err)
			return
		}
		//defer file.Close()

		// GET SUPPLIER
		r := csv.NewReader(file)
		r.Comma = []rune(conf.SuppliersCsvFormat.Delimeter)[0]
		r.LazyQuotes = true
		r.ReuseRecord = true
		_, err = r.Read()
		if err != nil {
			l.Error("csv.Reader.Read", err)
			file.Close()
			continue
		}

		sup, err := rep.GetSupplierByFilename(f.Name())
		if err != nil {
			l.Error("GetSupplierByFilename", errors.New(f.Name()+", err:"+err.Error()))
			file.Close()
			continue
		}

		var sucs, all int

		// PRODUCTS LOOP
		for {
			row, err := r.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				if errors.Is(err, csv.ErrFieldCount) {
					continue
				}
				l.Error("csv.Reader.Read", err)
				file.Close()
				continue fileloop
			}
			all++

			// GET BRAND ID
			normbrand := normstring(row[conf.SuppliersCsvFormat.BrandCol])
			if normbrand == "" {
				normbrand = "NO_BRAND"
			}
			brandid, err := rep.GetBrandIdByNorm(normstring(row[conf.SuppliersCsvFormat.BrandCol]))
			if err != nil {
				if errors.Is(err, ErrNotExists) {
					l.Debug("GetBrandIdByNorm", "not found brand "+row[conf.SuppliersCsvFormat.BrandCol]+", creating one")
					if brandid, err = rep.CreateBrand(strings.TrimSpace(row[conf.SuppliersCsvFormat.BrandCol]), []string{normstring(row[conf.SuppliersCsvFormat.BrandCol])}); err != nil {
						l.Error("GetBrandIdByNorm/CreateBrand", err)
						continue
					}
				} else {
					l.Error("GetBrandIdByNorm", errors.New("brand normname:"+normstring(row[conf.SuppliersCsvFormat.BrandCol])+", err: "+err.Error()))
					continue
				}
			}

			// CREATING ARTICUL
			normart := normstring(row[conf.SuppliersCsvFormat.ArticulCol])
			var alts []string
			if a, ok := altarts[normart]; ok {
				normart, alts = a.primary, a.alt
			}
			if err = rep.UpsertArticul(normart, brandid, alts); err != nil {
				l.Error("UpsertArticul", errors.New("normart: "+normart+", err"+err.Error()))
				continue
			}

			// GET SHIT
			price, err := strconv.ParseFloat(strings.Replace(row[conf.SuppliersCsvFormat.PriceCol], ",", ".", 1), 32)
			if err != nil {
				l.Error("ParseFloat/Price", errors.New("price:"+row[conf.SuppliersCsvFormat.PriceCol]+", product: "+row[conf.SuppliersCsvFormat.NameCol]+", err"+err.Error()))
				continue
			}
			quantity, err := strconv.Atoi(row[conf.SuppliersCsvFormat.QuantityCol])
			if err != nil {
				l.Error("Atoi/Quantity", errors.New("quantity:"+row[conf.SuppliersCsvFormat.QuantityCol]+", product: "+row[conf.SuppliersCsvFormat.NameCol]+", err"+err.Error()))
				continue
			}
			rest, err := strconv.Atoi(strings.Trim(row[conf.SuppliersCsvFormat.RestCol], "<>~"))
			if err != nil {
				l.Error("Atoi/Rest", errors.New("rest:"+row[conf.SuppliersCsvFormat.RestCol]+", product: "+row[conf.SuppliersCsvFormat.NameCol]+", err"+err.Error()))
				continue
			}

			// CREATE PRODUCT

			prodid, err := rep.GetOrCreateProduct(normart, sup.id, brandid, row[conf.SuppliersCsvFormat.NameCol], row[conf.SuppliersCsvFormat.PartnumCol], quantity)
			if err != nil {
				l.Error("GetOrCreateProduct", errors.New("product: "+row[conf.SuppliersCsvFormat.NameCol]+", err"+err.Error()))
				continue
			}
			if err = rep.UpsertActualPrice(prodid, uploadid, float32(price), rest); err != nil {
				l.Error("UpsertActualPrice", errors.New("product: "+row[conf.SuppliersCsvFormat.NameCol]+", err"+err.Error()))
				continue
			}
			if err = rep.InsertHistoryPrice(prodid, uploadid, float32(price), rest); err != nil {
				l.Error("InsertHistoryPrice", errors.New("product: "+row[conf.SuppliersCsvFormat.NameCol]+", err"+err.Error()))
				continue
			}
			sucs++
		}
		l.Debug(sup.Name, "successfully added "+strconv.Itoa(sucs)+" of "+strconv.Itoa(all)+" from "+f.Name())
		file.Close()

		if remove_processed {
			if err = os.Remove(conf.ProductsCsvPath + f.Name()); err != nil {
				l.Error("Remove", err)
			}
			l.Debug("Remove", "file removed: "+f.Name())
		}
	}
}

func createContextWithInterruptSignal(needunlock *bool, dirforunlock ...string) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		cancel()
		for _, n := range dirforunlock {
			locker.UnlockDir(n)
		}
	}()
	return ctx, cancel
}

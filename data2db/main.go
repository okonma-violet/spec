package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/okonma-violet/confdecoder"
)

type config struct {
	ProductsCsvPath             string
	SuppliersConfsPath          string
	BrandsFilePath              string
	AlternativeArticulsFilePath string
	SuppliersCsvFormatFilePath  string
	CategoriesFilePath          string

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
	mgrt := flag.Bool("migrate", false, "migrate tables")
	drp := flag.Bool("drop", false, "drop tables if exists")
	lb := flag.Bool("brands", false, "load brands from csv")
	ls := flag.Bool("sups", false, "load sups configs")
	lc := flag.Bool("cats", false, "load categories from csv")
	upl := flag.Bool("upload", false, "upload products from csvs")
	ctgrz := flag.Bool("categorize", false, "categorize")
	flag.Parse()

	rep := &repo{}
	err = rep.OpenDBRepository("postgres://ozon:q13471347@localhost:5432/ozondb")
	if err != nil {
		panic(err)
	}
	defer rep.db.Close(context.Background())
	conf.ProductsCsvPath += "/"
	conf.SuppliersConfsPath += "/"

	if *mgrt {
		if err = rep.Migrate(*drp); err != nil {
			panic(err)
		}
	}

	// CREATING BRANDS
	if *lb {
		if err := rep.loadBrandsFromFile(conf.BrandsFilePath); err != nil {
			panic(err)
		}
	}

	// CREATING SUPPLIERS
	if *ls {
		if err = rep.loadSuppliersConfigsFromDir(conf.SuppliersConfsPath); err != nil {
			panic(err)
		}
	}

	// CREATING CATEGORIES WITH KEYPHRASES
	if *lc {
		if err = rep.loadCategoriesWithKeyphrasesFromFile(conf.CategoriesFilePath); err != nil {
			panic(err)
		}
	}

	// CREATING PRODUCTS
	files, err := os.ReadDir(conf.ProductsCsvPath)
	if err != nil {
		panic(err)
	}

	// UPLOAD
	if *upl {
		fmt.Println("READ CSVs LOOP", "start")

		// CREATING ALTERNATIVE ARTICULES
		altarts, err := loadAlternativeArticulesFromFile(conf.AlternativeArticulsFilePath)
		if err != nil {
			panic(err)
		}
		fmt.Println("AlternativeArticules", "loaded unique arts with alts: "+strconv.Itoa(len(altarts)))
		for _, asd := range altarts {
			if len(asd.alt) > 1 {
				fmt.Println(asd)
			}
		}

		// CREATE UPLOAD
		uploadid, err := rep.CreateUpload()
		if err != nil {
			panic(err)
		}
		for _, f := range files {

			fmt.Println("READING FILE", f.Name())
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".csv") {
				fmt.Println("Format/ReadDir", "noncsv file founded "+f.Name())
				continue
			}
			file, err := os.Open(conf.ProductsCsvPath + f.Name())
			if err != nil {
				panic(err)
			}
			defer file.Close()

			// GET SUPPLIER
			r := csv.NewReader(file)
			r.Comma = []rune(conf.SuppliersCsvFormat.Delimeter)[0]
			r.LazyQuotes = true
			r.ReuseRecord = true
			_, err = r.Read()
			if err != nil {
				panic(err)
			}

			sup, err := rep.GetSupplierByFilename(f.Name())
			if err != nil {
				fmt.Println("GetSupplierByFilename", f.Name(), err)
				continue
			}

			var sucs, all int

			// LOOP
			for {
				row, err := r.Read()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					if errors.Is(err, csv.ErrFieldCount) {
						continue
					}
					panic(err)
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
						fmt.Println("GetBrandIdByNorm", "not found brand", row[conf.SuppliersCsvFormat.BrandCol]+",", "creating one")
						if brandid, err = rep.CreateBrand(strings.TrimSpace(row[conf.SuppliersCsvFormat.BrandCol]), []string{normstring(row[conf.SuppliersCsvFormat.BrandCol])}); err != nil {
							fmt.Println("GetBrandIdByNorm/CreateBrand", err)
							continue
						}
					} else {
						fmt.Println("GetBrandIdByNorm", row, normstring(row[conf.SuppliersCsvFormat.BrandCol]), err)
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
					panic(err)
				}

				// GET SHIT
				price, err := strconv.ParseFloat(strings.Replace(row[conf.SuppliersCsvFormat.PriceCol], ",", ".", 1), 32)
				if err != nil {
					fmt.Println("ParseFloat/Price", f.Name(), row[conf.SuppliersCsvFormat.PriceCol], row, err)
					continue
				}
				quantity, err := strconv.Atoi(row[conf.SuppliersCsvFormat.QuantityCol])
				if err != nil {
					fmt.Println("Atoi/Quantity", quantity, row, err)
					continue
				}
				rest, err := strconv.Atoi(strings.Trim(row[conf.SuppliersCsvFormat.RestCol], "<>~"))
				if err != nil {
					fmt.Println("Atoi/Rest", rest, f.Name(), row, err)
					continue
				}

				// CREATE PRODUCT

				prodid, err := rep.GetOrCreateProduct(normart, sup.id, brandid, row[conf.SuppliersCsvFormat.NameCol], row[conf.SuppliersCsvFormat.PartnumCol], quantity)
				if err != nil {
					fmt.Println("GetOrCreateProduct", row, err)
					continue
				}
				if err = rep.UpsertActualPrice(prodid, uploadid, float32(price), rest); err != nil {
					fmt.Println("UpsertPrice", row, err)
					continue
				}
				if err = rep.InsertHistoryPrice(prodid, uploadid, float32(price), rest); err != nil {
					fmt.Println("UpsertPrice", row, err)
					continue
				}
				sucs++
			}
			fmt.Println("\nsuccesfully added:", sucs, "of", all, " from ", f.Name()+"\n")
		}
		fmt.Println("READ CSVs LOOP", "done")
	}

	// CATEGORIZE UNCATEGORIZED
	if *ctgrz {
		if err = rep.Categorize(); err != nil {
			fmt.Println("Categorize", err)
		}
	}
}

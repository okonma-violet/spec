package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/okonma-violet/confdecoder"
)

type config struct {
	ProductsCsvPath            string
	SuppliersConfsPath         string
	BrandsFilePath             string
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

// DROPS AND RECREATES ALL TABLES ON MIGRATION !!!!!!!!!!!!!!
// TRUNCATES CATEGORIE'S KEYWORD'S FILE EVERY LAUNCH !!!!!!!!!!!!!!
func main() {
	conf := &config{}
	err := confdecoder.DecodeFile("config.txt", conf)
	if err != nil {
		panic("read config file err: " + err.Error())
	}
	if conf.ProductsCsvPath == "" || conf.SuppliersConfsPath == "" || conf.SuppliersCsvFormatFilePath == "" || conf.BrandsFilePath == "" {
		panic("ProductsCsvPath or SuppliersCsvFormatPath or SuppliersConfsPath or BrandsFilePath not specified in config.txt")
	}
	err = confdecoder.DecodeFile(conf.SuppliersCsvFormatFilePath, conf.SuppliersCsvFormat)
	if err != nil {
		panic("read config file err: " + err.Error())
	}
	if conf.SuppliersCsvFormat.Delimeter == "" {
		panic("bad SuppliersCsvFormat")
	}

	rep, err := OpenRepository("postgres://ozon:q13471347@localhost:5432/ozondb")
	if err != nil {
		panic(err)
	}
	defer rep.db.Close(context.Background())
	conf.ProductsCsvPath += "/"
	conf.SuppliersConfsPath += "/"
	if err = rep.Migrate(); err != nil {
		panic(err)
	}
	// CREATING BRANDS
	if err = rep.AddBrands(conf.BrandsFilePath); err != nil {
		panic(err)
	}
	// CREATING SUPPLIERS
	if err = rep.AddSuppliers(conf.SuppliersConfsPath); err != nil {
		panic(err)
	}

	// CREATING UNSORTED PRODUCTS
	files, err := os.ReadDir(conf.ProductsCsvPath)
	if err != nil {
		panic(err)
	}

	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".csv") {
			fmt.Println("Format/ReadDir", "noncsv file founded "+f.Name())
			continue
		}
		file, err := os.Open(conf.ProductsCsvPath + f.Name())
		if err != nil {
			panic(err)
		}
		defer file.Close()

		r := csv.NewReader(file)
		r.Comma = rune(conf.SuppliersCsvFormat.Delimeter[0])
		r.LazyQuotes = true
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
		for {
			all++
			row, err := r.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				if errors.Is(err, csv.ErrFieldCount) {
					break
				}
			}
			// CREATING ARTICUL
			normart := normstring(row[conf.SuppliersCsvFormat.ArticulCol])
			if err = rep.CreateArticul(normart); err != nil {
				// TODO:= ОШИБКА НЕ РАСПОЗНАЕТСЯ КАК *pgconn.PgError, хотя reflect.TypeOf(err).Elem().Name() = PgError
			}

			brandid, err := rep.GetBrandIdByNorm(normstring(strings.TrimSpace(row[conf.SuppliersCsvFormat.BrandCol])))
			if err != nil {
				fmt.Println(normstring(strings.TrimSpace(row[conf.SuppliersCsvFormat.BrandCol])), "|",
					strings.TrimSpace(row[conf.SuppliersCsvFormat.BrandCol]), "|",
					row[conf.SuppliersCsvFormat.BrandCol], "|",
					conf.SuppliersCsvFormat.BrandCol,
				)
				fmt.Println("GetBrandIdByNorm", row, err)
				continue
			}

			price, err := strconv.ParseFloat(strings.Replace(row[conf.SuppliersCsvFormat.PriceCol], ",", ".", 1), 32)
			if err != nil {
				fmt.Println("ParseFloat/Price", price, err)
				continue
			}
			quantity, err := strconv.Atoi(row[conf.SuppliersCsvFormat.QuantityCol])
			if err != nil {
				fmt.Println("Atoi/Quantity", quantity, err)
				continue
			}
			rest, err := strconv.Atoi(strings.Trim(row[conf.SuppliersCsvFormat.RestCol], "<>~"))
			if err != nil {
				fmt.Println("Atoi/Rest", rest, err)
				continue
			}

			_, err = rep.CreateUnsortedProduct(normart, sup.id, brandid, row[conf.SuppliersCsvFormat.NameCol], float32(price), row[conf.SuppliersCsvFormat.PartnumCol], quantity, rest)
			if err != nil {
				fmt.Println("err on creation of unsorted product:", err, normart, sup.id, brandid)
			} else {
				sucs++
			}
		}
		fmt.Println("succesfully added:", sucs, " of", all, " from", f.Name())
	}

	arts, err := rep.GetArticules()
	if err != nil {
		panic(err)
	}
	// FIND KEYWORDS
	keywords := make(map[string][]int)
	for i := 0; i < len(arts); i++ {
		unprods, err := rep.GetUnsortedProductsByArticul(arts[i])
		if err != nil {
			panic(err)
		}
		prodids := make([]int, 0, len(unprods))
		for k := 0; k < len(unprods); k++ {
			prodids = append(prodids, unprods[k].id)
		}
		kw := findKeywords(unprods, 2)
		for k := 0; k < len(kw); k++ {
			if _, ok := keywords[kw[k]]; !ok {
				keywords[kw[k]] = prodids
			}
		}
	}
	kwfile, err := os.OpenFile("categories_keywords.txt", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer kwfile.Close()
	ind := 1
	for kw := range keywords {
		kwfile.Write([]byte(strconv.Itoa(ind) + " " + kw))
	}

}

func findKeywords(prods []product, wordsnum int) []string {
	delim := rune(string(" ")[0])
	res := make([]string, 0)
loop:
	for i, lk, k, entries := 0, 0, 0, 0; i < len(prods); i, lk, k, entries = i+1, 0, 0, 0 {
		rw := []rune(strings.ToLower(strings.TrimSpace(prods[i].name)))

		for k < len(rw) {
			if rw[k] == delim {
				if k-lk > 3 {
					entries++
				}
				lk = k
			}
			if entries == wordsnum {
				break
			}
			k++
		}
		if entries < wordsnum {
			lk = k
		}
		if lk > 0 {
			rs := string(rw[:lk])
			for g := 0; g < len(res); g++ {
				if res[g] == rs {
					continue loop
				}
			}
			res = append(res, rs)
		}
	}
	return res
}

var articulnormrx = regexp.MustCompile("[^а-яa-z0-9]|-")

func normstring(s string) string {
	return articulnormrx.ReplaceAllString(strings.ToLower(s), "")
}

func (r *repo) AddBrands(filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	rdr := csv.NewReader(file)
	rdr.Comma = rune(string(",")[0])
	rdr.LazyQuotes = true
	if _, err := r.db.Exec(context.Background(), "INSERT INTO brands(name,norm) values($1,$2)", "NO_BRAND", []string{"NO_BRAND"}); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return ErrDuplicate
		}
		return err
	}
	var dups int
	var sucs int
	for {
		row, err := rdr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if len(row) < 3 {
			fmt.Println("CreateBrand", "less than 3 num of fields in a row:", row)
			continue
		}
		name := strings.TrimSpace(row[1])
		if len(name) == 0 {
			fmt.Println("CreateBrand", "empty name in a row:", row)
		}
		norms := strings.Split(row[2], ",")
		for i := 0; i < len(norms); i++ {
			norms[i] = normstring(norms[i])
			if len(norms[i]) == 0 {
				norms = norms[:i+copy(norms[i:], norms[i+1:])]
			}
		}
		if len(norms) == 0 {
			norms = append(norms, normstring(name))
		}
		if err := r.CreateBrand(name, norms); err != nil {
			if errors.Is(err, ErrDuplicate) {
				dups++
				continue
			}
			return err
		}
		sucs++
	}
	fmt.Println("CreateBrand", "added", sucs, "brands, duplicates:", dups)

	return nil
}

func (r *repo) AddSuppliers(path string) error {
	files, err := os.ReadDir(path)
	if err != nil {
		panic(err)
	}
	var dups int
	var sucs int
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".txt") {
			fmt.Println("AddSuppliers", "nontxt founded:", f.Name())
			continue
		}

		sfrm := supplier{}
		err := confdecoder.DecodeFile(path+f.Name(), &sfrm)
		if err != nil {
			panic("read supplier's config file err: " + err.Error())
		}
		if sfrm.Name == "" || sfrm.Filename == "" {
			fmt.Println("AddSuppliers", "no data in supplier's config file:", f.Name())
			continue
		}
		if _, err = r.CreateSupplier(
			sfrm.Name,
			sfrm.Email,
			sfrm.Filename,
			sfrm.Delimiter,
			sfrm.Quotes == 1,
			sfrm.FirstRow,
			sfrm.BrandCol,
			sfrm.ArticulCol,
			sfrm.NameCol,
			sfrm.PartnumCol,
			sfrm.PriceCol,
			sfrm.QuantityCol,
			sfrm.RestCol); err != nil {
			if errors.Is(err, ErrDuplicate) {
				dups++
				continue
			}
			return err
		}
		sucs++
	}
	fmt.Println("AddSuppliers", "added", sucs, "suppliers, duplicates:", dups)
	return nil
}

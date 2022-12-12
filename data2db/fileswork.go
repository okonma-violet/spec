package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/pgconn"
	"github.com/okonma-violet/confdecoder"
)

func (r *repo) AddBrands(filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	rdr := csv.NewReader(file)
	rdr.Comma = []rune(",")[0]
	rdr.LazyQuotes = true
	if _, err := r.db.Exec(context.Background(), "INSERT INTO brands(name,norm) values($1,$2)", "NO_BRAND", []string{"NO_BRAND"}); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code != pgerrcode.UniqueViolation {
			return err
		}
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
		name := strings.TrimSpace(row[1])
		if len(name) == 0 {
			fmt.Println("CreateBrand", "empty name in a row:", row)
			continue
		}
		var norms []string
		if len(row) < 3 {
			norms = make([]string, 0)
		} else {
			norms = strings.Split(row[2], ",")
		}
		for i := 0; i < len(norms); i++ {
			norms[i] = normstring(norms[i])
			if len(norms[i]) == 0 {
				norms = norms[:i+copy(norms[i:], norms[i+1:])]
			}
		}
		if len(norms) == 0 {
			norms = append(norms, normstring(name))
		}
		if _, err = r.CreateBrand(name, norms); err != nil {
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

// func keywords2csv(filepath string, keywords map[string]struct{}) error {
// 	if !strings.HasSuffix(filepath, ".csv") {
// 		panic("not a csv")
// 	}
// 	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
// 	if err != nil {
// 		return (err)
// 	}
// 	defer file.Close()

// 	w := csv.NewWriter(file)
// 	w.Comma = []rune(";")[0]
// 	w.UseCRLF = true

// 	ind := 1
// 	for kw := range keywords {
// 		if err = w.Write([]string{strconv.Itoa(ind), kw, ""}); err != nil {
// 			return err
// 		}
// 		ind++
// 	}
// 	return nil
// }

func (rep *repo) addCategoriesWithKeyphrases2db(filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return (err)
	}
	defer file.Close()

	r := csv.NewReader(file)
	r.Comma = []rune(";")[0]
	r.Comment = []rune("#")[0]

	data, err := r.ReadAll()
	if err != nil {
		return err
	}

	for i, row := range data {
		if len(row[0]) == 0 || len(row[1]) == 0 {
			fmt.Println("addKeywordsWithCats2db", "skip row ", i, " on empty catname kphrase")
			continue
		}
		norm := normstring(row[1])
		if len(norm) == 0 {
			return errors.New("gets empty normname of cat on line " + strconv.Itoa(i+1))
		}
		catid, err := rep.AddCategory(row[1], norm)
		if err != nil {
			if errors.Is(err, ErrDuplicate) {
				catid, err = rep.GetCategoryIdByNorm(normstring(row[1]))
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		kps := strings.Split(row[2], ",")
		for k := 0; k < len(kps); k++ {
			kp := strings.TrimSpace(strings.ToLower(kps[k]))
			if len(kp) == 0 {
				continue
			}
			if err = rep.AddCategoryKeyphrase(catid, kp); err != nil && !errors.Is(err, ErrDuplicate) {
				return err
			}
		}
	}
	return nil
}

package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/okonma-violet/confdecoder"
)

type config struct {
	CsvPath string

	OutFormat []int // delim(rune), brand col, articul col, name col, partnum col, price col, quantity col
}

// DROPS AND RECREATES ALL TABLES ON MIGRATION !!!!!!!!!!!!!!
func main() {
	conf := &config{}
	err := confdecoder.DecodeFile("config.txt", conf)
	if err != nil {
		panic("read config file err: " + err.Error())
	}
	if conf.CsvPath == "" {
		panic("no CsvPath specified in config.txt")
	}
	if conf.OutFormat == nil || len(conf.OutFormat) != 7 {
		panic("no OutFormat specified or num of values dont match (must be 7 values)")
	}
	rep, err := OpenRepository("postgres://ozon:q13471347@localhost:5432/ozondb")
	if err != nil {
		panic(err)
	}
	defer rep.db.Close(context.Background())
	conf.CsvPath += "/"
	if err = rep.Migrate(); err != nil {
		panic(err)
	}

	files, err := os.ReadDir(conf.CsvPath)
	if err != nil {
		panic(err)
	}

	for si, f := range files {
		si++ // FOR TEST
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".csv") {
			fmt.Println("Format/ReadDir", "noncsv file founded "+f.Name())
			continue
		}
		file, err := os.Open(conf.CsvPath + f.Name())
		if err != nil {
			panic(err)
		}
		defer file.Close()
		r := csv.NewReader(file)
		r.Comma = rune(conf.OutFormat[0])
		r.LazyQuotes = true
		_, err = r.Read()
		if err != nil {
			panic(err)
		}
		for i := 0; i < 20; i++ { // FOR TEST
			row, err := r.Read()
			if err != nil {
				panic(err)
			}
			normart := normstring(row[conf.OutFormat[2]])
			if err = rep.CreateArticul(normart); err != nil {
				var pgErr *pgconn.PgError
				fmt.Println(err, errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation)
			}

			_, err = rep.CreateUnsortedProduct(row[conf.OutFormat[3]], row[conf.OutFormat[1]], normart, row[conf.OutFormat[4]], si)
			if err != nil {
				fmt.Println("err on creation of unsorted product:", err)
			}
		}
	}

	arts, err := rep.GetArticules()
	if err != nil {
		panic(err)
	}

	for i := 0; i < len(arts); i++ {
		unprods, err := rep.GetUnsortedProductsByArticul(arts[i])
		if err != nil {
			panic(err)
		}
		kw := findKeywords(unprods, 2)
		for k, p := range unprods {
			fmt.Println("-----------------------------")
			fmt.Println(k, "prod:", p)
		}
		fmt.Println("keywords:", kw)
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

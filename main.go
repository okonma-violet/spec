package main

import (
	"encoding/csv"
	"os"
	"regexp"
	"strconv"
	"strings"
)

func main() {
	println("CONVERT")
	convert("prices/")
	println("CONVERT DONE")

	println("MOVE CSV")
	movefiles(".", "prices/csv/", ".csv")
	println("MOVE CSV DONE")

	//println("LOAD BRANDS")

	//println("LOAD BRANDS DONE")
}

func convert(path string) {
	files, err := os.ReadDir(path)
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if f.IsDir() || (!strings.HasSuffix(f.Name(), ".xls") && !strings.HasSuffix(f.Name(), ".xlsx")) {
			continue
		}
		if err := converttocsv(path + "/" + f.Name()); err != nil {
			panic(err)
		}
		println("converted", f.Name())
	}
}

func movefiles(currpath string, newpath string, files_ext string) {
	files, err := os.ReadDir(currpath)
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), files_ext) {
			if err = os.Rename(currpath+"/"+f.Name(), newpath+"/"+f.Name()); err != nil {
				panic(err)
			}
		}
	}
}

type repo struct {
	brandsById   map[int]*brand
	brandsByNorm map[string]*brand
	//categiriesByName   map[string]*category
	categiriesById    map[int]*category
	productsByArticul map[string]*product

	ngkArticules map[string][]string
}

type brand struct {
	id   int
	name string
	norm []string
}

type category struct {
	id      int
	name    string
	phrases []string
}

type product struct {
	id          int
	brand_id    int
	category_id int
	name        string
	articul     string
}

func (r *repo) loadbrands(filepath string) {
	rows, err := readCSV(filepath)
	if err != nil {
		panic(err)
	}
	r.brandsByNorm = make(map[string]*brand)
	r.brandsById = make(map[int]*brand)
	for _, fields := range rows {
		if len(fields) < 3 {
			panic("less than 3 num of fields")
		}
		if len(fields[1]) == 0 {
			panic("empty brand name")
		}
		id, err := strconv.Atoi(fields[0])
		if err != nil {
			panic(err)
		}
		norms := strings.Split(fields[2], ",")
		for i := 0; i < len(norms); i++ {
			norms[i] = normstring(norms[i])
			if len(norms[i]) == 0 {
				norms = norms[:i+copy(norms[i:], norms[i+1:])]
			}
		}
		if len(norms) == 0 {
			norms = append(norms, normstring(fields[1]))
		}
		for i := 0; i < len(norms); i++ {
			if brnd, ok := r.brandsByNorm[norms[i]]; ok {
				brnd.norm = append(brnd.norm, norms[i])
			} else {
				r.brandsByNorm[norms[i]] = &brand{
					id:   id,
					name: fields[1],
					norm: []string{norms[i]},
				}
			}
			if brnd, ok := r.brandsById[id]; ok {
				brnd.norm = append(brnd.norm, norms[i])
			} else {
				r.brandsById[id] = &brand{
					id:   id,
					name: fields[1],
					norm: []string{norms[i]},
				}
			}
		}
	}
}

func (r *repo) loadcategories(filepath string) {
	rows, err := readCSV(filepath)
	if err != nil {
		panic(err)
	}
	r.categiriesById = make(map[int]*category)
	for _, fields := range rows {
		if len(fields) < 2 {
			panic("less than 2 num of fields")
		}
		if len(fields[1]) == 0 {
			panic("empty category name")
		}
		id, err := strconv.Atoi(fields[0])
		if err != nil {
			panic(err)
		}
		phrases := strings.Split(fields[1], " ")
		for i := 0; i < len(phrases); i++ {
			phrases[i] = unplural(strings.ToLower(phrases[i]))
			if len(phrases) == 0 {
				continue
			}
		}
		if len(phrases) == 0 {
			panic("no phrases founded")
		}
		if _, ok := r.categiriesById[id]; ok {
			panic("id doubled")
		} else {
			r.categiriesById[id] = &category{
				id:      id,
				name:    fields[1],
				phrases: phrases,
			}
		}

	}
}

func (r *repo) loadproducts(path string) {
	files, err := os.ReadDir(path)
	if err != nil {
		panic(err)
	}
	r.productsByArticul = make(map[string]*product)
	inds := make([]int, 0, 3)
	rxs := []*regexp.Regexp{regexp.MustCompile(`производитель|бренд`), regexp.MustCompile(`артикул`), regexp.MustCompile(`наименование|название`)}
	for _, f := range files {
		if f.IsDir() || (!strings.HasSuffix(f.Name(), ".csv")) {
			continue
		}
		rows, err := readCSV(path + "/" + f.Name())
		if err != nil {
			panic(err)
		}
		if len(rows) == 0 {
			panic("empty file " + f.Name())
		}
		for i := 0; i < len(rxs); i++ {
			for k := 0; k < len(rows[0]); k++ {
				if rxs[i].MatchString(strings.ToLower(rows[0][k])) {
					inds = append(inds, i)
					break
				}
			}
		}
		if len(inds) != 3 {
			panic("not found one of regs")
		}
	loop:
		for i := 1; i < len(rows); i++ {
			brand := strings.ToLower(rows[i][inds[0]])
			art := rows[i][inds[1]]
			name := rows[i][inds[2]]

			if brand == "ngk" {
				if ngkarts, ok := r.ngkArticules[art]; ok {
					for k := 0; k < len(ngkarts); k++ {
						if _, ok := r.productsByArticul[ngkarts[k]]; ok {
							println("founded ngk already added product by second articul")
							continue loop
						}
					}
				}
			}
			if _, ok := r.productsByArticul[art]; ok {
				println("founded already added articul")
				continue loop
			}

		}

		inds = inds[:0]
	}

}

// ONLY " DELIMITERS AND ONLY , SEPARATORS
func readCSV(path string) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *repo) loadNGKarticules(filepath string) {
	filedata, err := os.ReadFile(filepath)
	if err != nil {
		panic(err)
	}
	rx := regexp.MustCompile(`[0-9A-Za-z-]+`)
	r.ngkArticules = make(map[string][]string)
	rows := strings.Split(string(filedata), "\n")
	for i := 0; i < len(rows); i++ {
		if arts := rx.FindAllString(rows[i], -1); len(arts) < 2 {
			continue
		} else {
			if _, ok := r.ngkArticules[arts[0]]; ok {
				r.ngkArticules[arts[0]] = append(r.ngkArticules[arts[0]], arts[1])
			} else {
				r.ngkArticules[arts[0]] = []string{arts[1]}
			}
			if _, ok := r.ngkArticules[arts[1]]; ok {
				r.ngkArticules[arts[1]] = append(r.ngkArticules[arts[1]], arts[0])
			} else {
				r.ngkArticules[arts[1]] = []string{arts[0]}
			}
		}
	}
}

func unplural(str string) string {
	var foo string
	if len(str) < 2 {
		if len(str) == 0 {
			return str
		}
		goto single
	}
	foo = str[len(str)-2:]
	if foo == "ья" || foo == "ые" || foo == "ие" || foo == "ых" || foo == "их" || foo == "ой" || foo == "ий" || foo == "ый" || foo == "ая" || foo == "ое" || foo == "яя" || foo == "ее" {
		return str[:len(str)-2]
	}
single:
	foo = str[len(str)-1:]
	if foo == "ы" || foo == "а" || foo == "я" || foo == "и" {
		return str[:len(str)-1]
	}
	return str
}

var articulnormrx = regexp.MustCompile("[^а-яa-z0-9]")

func normstring(s string) string {
	return articulnormrx.ReplaceAllString(strings.ToLower(s), "")
}

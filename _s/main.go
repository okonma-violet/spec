package main

import (
	"encoding/csv"
	"os"
	"regexp"
	"strconv"
	"strings"
)

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

var articulnormrx = regexp.MustCompile("[^а-яa-z0-9]")

func main() {
	println("CONVERT")
	convert("test/prices/")
	println("CONVERT DONE")

	println("MOVE CSV")
	movefiles(".", "test/prices/csv/", ".csv")
	println("MOVE CSV DONE")

	r := &repo{}

	println("LOAD BRANDS")
	r.loadbrands("test/refs/brands.csv")
	println("LOAD BRANDS DONE")

	println("LOAD CATEGORIES")
	r.loadcategories("test/refs/categories.csv")
	println("LOAD CATEGORIES DONE")

	println("LOAD NGK ARTS")
	r.loadNGKarticules("test/refs/ngk.txt")
	println("LOAD NGK ARTS DONE")

	println("LOAD PRODUCTS")
	r.loadproducts("test/prices/csv/")
	println("LOAD PRODUCTS DONE")

	println("CATEGORIZING")
	r.categorizeAll()
	println("CATEGORIZING DONE")

	println("UPLOAD TO CSV")
	r.uploadToCSV("test/res/")
	println("UPLOAD TO CSV DONE")

	// fmt.Println("\n-----------------\n")
	// var i int
	// fmt.Println("\nbrands by id")
	// for j, k := range r.brandsById {
	// 	if i < 10 {
	// 		fmt.Println("++ id", j, k)
	// 		i++
	// 	} else {
	// 		break
	// 	}
	// }
	// fmt.Println("\nbrands by norm")
	// for j, k := range r.brandsByNorm {
	// 	if i > 0 {
	// 		fmt.Println("-- norm", j, k)
	// 		i--
	// 	} else {
	// 		break
	// 	}
	// }
	// fmt.Println("\ncategories by id")
	// for j, k := range r.categiriesById {
	// 	fmt.Println("$$ id", j, k)
	// }
	// fmt.Println("\nproducts by art")
	// for j, k := range r.productsByArticul {
	// 	fmt.Println("## art", j, k)
	// }
}

func (r *repo) uploadToCSV(path string) {
	f, err := os.OpenFile(path+"/products.csv", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	wr := csv.NewWriter(f)
	prds := make([][]string, len(r.productsByArticul)+1)
	prds[0] = []string{"id", "brand_id", "category_id", "name", "articul"}
	for _, prd := range r.productsByArticul {
		prds[prd.id] = []string{strconv.Itoa(prd.id), strconv.Itoa(prd.brand_id), strconv.Itoa(prd.category_id), prd.name, prd.articul}
	}
	err = wr.WriteAll(prds)
	if err != nil {
		panic(err)
	}
	f.Close()

	f, err = os.OpenFile(path+"/category.csv", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	wr = csv.NewWriter(f)
	cats := make([][]string, len(r.categiriesById)+1)
	cats[0] = []string{"id", "name", "phrases"}
	for _, cat := range r.categiriesById {
		cats[cat.id] = []string{strconv.Itoa(cat.id), cat.name, strings.Join(cat.phrases, ", ")}
	}
	err = wr.WriteAll(cats)
	if err != nil {
		panic(err)
	}
	f.Close()

	f, err = os.OpenFile(path+"/brands.csv", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	wr = csv.NewWriter(f)
	brds := make([][]string, len(r.brandsById)+1)
	brds[0] = []string{"id", "name", "norm"}
	for _, brd := range r.brandsById {
		brds[brd.id] = []string{strconv.Itoa(brd.id), brd.name, strings.Join(brd.norm, ", ")}
	}
	err = wr.WriteAll(brds)
	if err != nil {
		panic(err)
	}
	f.Close()
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

func (r *repo) categorize(prod *product) {
	var c, max_subm, max_subm_id int
	for cat_id, cat := range r.categiriesById {
		if c = countMaxMatchLength(prod.name, cat.phrases); c > max_subm {
			max_subm = c
			max_subm_id = cat_id
		}
	}
	if max_subm_id > 0 {
		prod.category_id = max_subm_id
		max_subm_id, max_subm = 0, 0
	} else {
		println("did not found any category for product "+prod.name, ", added to \"UNKNOWN CATEGORY\" ")
		prod.category_id = 1
	}
}

// rewrites all previous categorizing
func (r *repo) categorizeAllHard() {
	for _, prod := range r.productsByArticul {
		r.categorize(prod)
	}
}

// only categorizes products with unknown category (if founds one) and uncategorized products
func (r *repo) categorizeAll() {
	for _, prod := range r.productsByArticul {
		if prod.category_id > 1 {
			r.categorize(prod)
		}
	}
}

// substr must be lowered
func countMaxMatchLength(str string, substr []string) (count int) {
	var subcnt int
	subrs := make([][]rune, len(substr))
	for i := 0; i < len(substr); i++ {
		subrs[i] = []rune(substr[i])
	}
	r := []rune(strings.ToLower(str))
	for i, maxsubcnt := 0, 0; i < len(subrs); i, maxsubcnt = i+1, 0 {

		//fmt.Println("\n$$$ word", string(subrs[i]))
		//fmt.Println("--- subcnt-- =", 0, ",was", subcnt, ",maxsubcnt =", maxsubcnt)
		subcnt = 0
		for k, j := 0, 0; k < len(r) && j < len(subrs[i]); k++ {
			//fmt.Println("+++ compare ", string(r[:k])+"["+string(r[k])+"]"+string(r[k+1:]), "with", string(subrs[i][:j])+"["+string(subrs[i][j])+"]"+string(subrs[i][j+1:]))
			if r[k] != subrs[i][j] {
				if subcnt > maxsubcnt {
					//fmt.Println("=== maxsubcnt now", subcnt, ",was", maxsubcnt)
					maxsubcnt = subcnt
				}
				subcnt = 0
				j = 0
			} else {
				subcnt++
				j++
				//fmt.Println("--- subcnt++ =", subcnt)
			}
		}
		if subcnt > maxsubcnt {
			//fmt.Println("=+= maxsubcnt now", subcnt, ",was", maxsubcnt)
			maxsubcnt = subcnt
		}
		//fmt.Println("--- maxsubcnt =", maxsubcnt)
		if maxsubcnt > 2 {
			//fmt.Println("### count now", count+maxsubcnt, ",was", count)
			count += maxsubcnt
		}
	}
	//fmt.Println("total count", count, subcnt, string(r), substr)
	return
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

func normstring(s string) string {
	return articulnormrx.ReplaceAllString(strings.ToLower(s), "")
}

package main

import (
	"os"
	"regexp"
	"strings"
)

func (r *repo) loadbrands(filepath string) {
	rows, err := readCSV(filepath)
	if err != nil {
		panic(err)
	}
	r.brandsByNorm = make(map[string]*brand)
	r.brandsById = make(map[int]*brand)
	curind := 2

	def := &brand{id: 1, name: "NO_BRAND", norm: []string{"NO_BRAND"}}
	r.brandsById[1] = def
	r.brandsByNorm[def.norm[0]] = def

	for _, fields := range rows {
		if len(fields) < 3 {
			panic("less than 3 num of fields")
		}
		name := strings.TrimSpace(fields[1])
		if len(name) == 0 {
			panic("empty brand name")
		}
		norms := strings.Split(fields[2], ",")
		for i := 0; i < len(norms); i++ {
			norms[i] = normstring(norms[i])
			if len(norms[i]) == 0 {
				norms = norms[:i+copy(norms[i:], norms[i+1:])]
			}
		}
		if len(norms) == 0 {
			norms = append(norms, normstring(name))
		}
		for i := 0; i < len(norms); i++ {
			if brnd, ok := r.brandsByNorm[norms[i]]; ok {
				brnd.norm = append(brnd.norm, norms[i])
			} else {
				r.brandsByNorm[norms[i]] = &brand{
					id:   curind,
					name: name,
					norm: []string{norms[i]},
				}
			}
			if brnd, ok := r.brandsById[curind]; ok {
				brnd.norm = append(brnd.norm, norms[i])
			} else {
				r.brandsById[curind] = &brand{
					id:   curind,
					name: name,
					norm: []string{norms[i]},
				}
			}
		}
		curind++
	}
}

func (r *repo) loadcategories(filepath string) {
	rows, err := readCSV(filepath)
	if err != nil {
		panic(err)
	}
	r.categiriesById = make(map[int]*category)
	currid := 2
	r.categiriesById[1] = &category{id: 1, name: "UNKNOWN CATEGORY", phrases: []string{}}
	for _, fields := range rows {
		if len(fields) < 1 {
			panic("less than 1 num of fields")
		}

		phrases := strings.Split(fields[0], " ")
		for i := 0; i < len(phrases); i++ {
			phrases[i] = unplural(strings.ToLower(phrases[i]))
			if len(phrases) == 0 {
				continue
			}
		}
		if len(phrases) == 0 {
			panic("no phrases founded")
		}
		if _, ok := r.categiriesById[currid]; ok {
			panic("id doubled")
		} else {
			r.categiriesById[currid] = &category{
				id:      currid,
				name:    strings.TrimSpace(fields[0]),
				phrases: phrases,
			}
		}
		currid++
	}
}

func (r *repo) loadproducts(path string) {
	files, err := os.ReadDir(path)
	if err != nil {
		panic(err)
	}
	r.productsByArticul = make(map[string]*product)
	inds := make([]int, 0, 3)
	rxs := []*regexp.Regexp{regexp.MustCompile(`производитель|бренд`), regexp.MustCompile(`артикул`), regexp.MustCompile(`наименование|название|имя`)}
	curind := 1
	var brandid int
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
					inds = append(inds, k)
					break
				}
			}
		}
		if len(inds) != 3 {
			panic("not found one of regs")
		}
	loop:
		for i := 1; i < len(rows); i++ {
			brand_normname := normstring(rows[i][inds[0]])
			art := rows[i][inds[1]]
			name := strings.TrimSpace(rows[i][inds[2]])

			if brand_normname == "ngk" {
				if ngkarts, ok := r.ngkArticules[art]; ok {
					for k := 0; k < len(ngkarts); k++ {
						if _, ok := r.productsByArticul[ngkarts[k]]; ok {
							println("founded ngk already added product by second articul")
							continue loop
						}
					}
				}
			}
			if b, ok := r.brandsByNorm[brand_normname]; ok {
				brandid = b.id
			} else {
				println("brand not found by norm " + brand_normname)
				continue loop
			}

			if _, ok := r.productsByArticul[art]; ok {
				println("founded already added articul " + art)
				continue loop
			} else {
				r.productsByArticul[art] = &product{
					id:       curind,
					brand_id: brandid,
					name:     name,
					articul:  art,
				}
			}
			curind++
		}

		inds = inds[:0]
	}
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
			//if _, ok := r.ngkArticules[arts[0]]; ok {
			r.ngkArticules[arts[0]] = append(r.ngkArticules[arts[0]], arts[1])
			//} else {
			//	r.ngkArticules[arts[0]] = []string{arts[1]}
			//}
			//if _, ok := r.ngkArticules[arts[1]]; ok {
			r.ngkArticules[arts[1]] = append(r.ngkArticules[arts[1]], arts[0])
			//} else {
			//	r.ngkArticules[arts[1]] = []string{arts[0]}
			//}
		}
	}
}

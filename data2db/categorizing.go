package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

type comparativeArray []*keyPhrase

type keyPhrase struct {
	phrase string
	catid  int
}

func (ca comparativeArray) Len() int {
	return len(ca)
}

// BY LEN
func (ca comparativeArray) Less(i, j int) bool {
	return len(ca[i].phrase) > len(ca[j].phrase) // reversed for desc
}

// // BY WORDS COUNT
//
//	func (ca comparativeArray) Less(i, j int) bool {
//		return strings.Count(ca[i].phrase, " ")+1 > strings.Count(ca[j].phrase, " ")+1 // reversed for desc
//	}

func (ca comparativeArray) Swap(i, j int) {
	b := ca[i]
	ca[i] = ca[j]
	ca[j] = b
}

func (r *repo) Categorize() error {
	var keysarr comparativeArray
	keysarr, err := r.GetCategoriesKeyphrases()
	if err != nil {
		panic(err)
	}
	sort.Sort(keysarr)

	ars, err := r.GetUncategorizedArticulesWithBrandids()
	if err != nil {
		return err
	}

	for i, k := range ars {
		fmt.Println(i, k)
	}

	var cated int
	for i := 0; i < len(ars); i++ {
		names, err := r.GetProductsNamesByArtAndBrand(ars[i].articul, ars[i].brandid)
		if err != nil {
			return err
		}
		catid := 0
		for k := 0; k < len(names); k++ {
			if kp := keysarr.findMatch(names[k]); kp != nil {
				catid = kp.catid
				break
			}
		}
		if catid != 0 {
			if err = r.UpdateArticulCategory(ars[i].articul, ars[i].brandid, catid); err != nil {
				return err
			}
			cated++
		}
	}
	fmt.Println("Categorized", cated)
	return nil
}

var nonrussianrx = regexp.MustCompile(`[^а-я ]`)
var specialrx = regexp.MustCompile(`[\(\)\[\]\{\}\\\/]`)

func normspecialsrx(s string) string {
	return specialrx.ReplaceAllString(strings.ToLower(s), " ")
}

// DO NOT LOWERS STR
func normrussifyrx(s string) string {
	return nonrussianrx.ReplaceAllString(s, "")
}
func containsOnlyRussian(s string) bool {
	return !nonrussianrx.MatchString(s)
}

func (ca comparativeArray) findMatch(str string) *keyPhrase {
	var kwords, words []string
	str = normspecialsrx(str)
	for i, eq, s := 0, 0, str; i < len(ca); i, eq, s = i+1, 0, str {
		if containsOnlyRussian(ca[i].phrase) {
			s = normrussifyrx(s)
		}
		kwords = strings.Split(ca[i].phrase, " ")
		words = strings.Split(s, " ")

		for wi := 0; wi < len(words); wi++ {
			if len(words[wi]) == 0 {
				continue
			}
			for ki := 0; ki < len(kwords); ki++ {
				if strings.Compare(words[wi], kwords[ki]) == 0 {
					eq++
					break
				}
			}
		}
		if eq == len(kwords) {
			return ca[i]
		}
	}
	return nil
}

// // STRICT ORDER
// func (ca comparativeArray) findMatch(str string) *keyPhrase {
// 	var kwords, words []string
// 	str = normspecialsrx(str)
// 	for i, eq, s := 0, 0, str; i < len(ca); i, eq, s = i+1, 0, str {
// 		if containsOnlyRussian(ca[i].phrase) {
// 			s = normrussifyrx(s)
// 		}
// 		kwords = strings.Split(ca[i].phrase, " ")
// 		words = strings.Split(s, " ")

// 		for wi, ki := 0, 0; wi < len(words) && ki < len(kwords); wi++ {
// 			if len(words[wi]) == 0 {
// 				continue
// 			}
// 			if len(kwords[ki]) == 0 {
// 				ki++
// 				eq++
// 				continue
// 			}
// 			if strings.Compare(words[wi], kwords[ki]) == 0 {
// 				eq++
// 				ki++
// 				wi--
// 			} else {
// 				if eq != 0 {
// 					ki = 0
// 				}
// 			}
// 		}
// 		if eq == len(kwords) {
// 			return ca[i]
// 		}
// 	}
// 	return nil
// }

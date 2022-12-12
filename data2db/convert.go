package main

import (
	"regexp"
	"strings"
)

var articulnormrx = regexp.MustCompile("[^а-яa-z0-9]")

func normstring(s string) string {
	return articulnormrx.ReplaceAllString(strings.ToLower(s), "")
}

// func unplural(str string) string {
// 	rs := []rune(str)
// 	if len(rs) < 2 {
// 		if len(rs) == 0 {
// 			return str
// 		}
// 		goto single
// 	}
// 	if (rs[len(rs)-2] == []rune("ы")[0] && (rs[len(rs)-1] == []rune("е")[0] || rs[len(rs)-1] == []rune("х")[0] || rs[len(rs)-1] == []rune("й")[0])) ||
// 		(rs[len(rs)-2] == []rune("и")[0] && (rs[len(rs)-1] == []rune("е")[0] || rs[len(rs)-1] == []rune("х")[0] || rs[len(rs)-1] == []rune("й")[0])) ||
// 		(rs[len(rs)-2] == []rune("о")[0] && (rs[len(rs)-1] == []rune("е")[0] || rs[len(rs)-1] == []rune("й")[0])) ||
// 		(rs[len(rs)-2] == []rune("ь")[0] && (rs[len(rs)-1] == []rune("я")[0])) ||
// 		(rs[len(rs)-2] == []rune("а")[0] && (rs[len(rs)-1] == []rune("я")[0])) ||
// 		(rs[len(rs)-2] == []rune("я")[0] && (rs[len(rs)-1] == []rune("я")[0])) ||
// 		(rs[len(rs)-2] == []rune("е")[0] && (rs[len(rs)-1] == []rune("е")[0])) {
// 		return string(rs[:len(rs)-2])

// 	}
// single:
// 	if rs[len(rs)-1] == []rune("ы")[0] || rs[len(rs)-1] == []rune("а")[0] || rs[len(rs)-1] == []rune("я")[0] || rs[len(rs)-1] == []rune("и")[0] {
// 		return string(rs[:len(rs)-1])
// 	}
// 	return str
// }

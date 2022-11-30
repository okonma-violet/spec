package main

import (
	"context"
	"errors"

	"github.com/jackc/pgx"
)

var ErrDuplicate = errors.New("duplicates")

type repo struct {
	db *pgx.Conn
}

func OpenRepository(connectionString string) (*repo, error) {
	db, err := pgx.Connect(context.Background(), connectionString)
	if err != nil {
		return nil, err
	}
	return &repo{db: db}, nil
}

func (rep *repo) Migrate() error {
	query := `
	CREATE TABLE IF NOT EXISTS "suppliers" (
		"id"	SERIAL NOT NULL PRIMARY KEY,
		"name"	TEXT NOT NULL,
		"email"	TEXT NOT NULL,
		"filename"	TEXT NOT NULL,
		"delimiter"	TEXT NOT NULL,
		"quotes"	BOOL NOT NULL,
		"firstrow"	INTEGER NOT NULL,
		"brandcol"	INTEGER NOT NULL,
		"articulcol"	INTEGER NOT NULL,
		"namecol"	INTEGER ARRAY NOT NULL,
		"partnumcol"	INTEGER NOT NULL,
		"pricecol"	INTEGER NOT NULL,
		"quantitycol"	INTEGER NOT NULL,
		"restcol"	INTEGER NOT NULL,
		UNIQUE("filename")
	);
	`

	_, err := rep.db.Exec(context.Background(), query)
	return err
}

type supplier struct {
	id          int
	Name        string
	Email       string
	Filename    string
	Delimiter   string
	Quotes      int
	FirstRow    int
	BrandCol    int
	ArticulCol  int
	NameCol     []int
	PartnumCol  int
	PriceCol    int
	QuantityCol int
	RestCol     int
}

func (r *repo) GetSuppliersByNames() (map[string]supplier, error) {
	var quotes bool
	res := make(map[string]supplier)
	if rows, err := r.db.Query(context.Background(), "SELECT id,name,email,filename,delimiter,quotes,firstrow,brandcol,articulcol,namecol,partnumcol,pricecol,quantitycol,restcol FROM suppliers"); err != nil {
		return nil, err
	} else {
		for rows.Next() {
			var sup supplier
			if err = rows.Scan(
				&sup.id, &sup.Name, &sup.Email, &sup.Filename, &sup.Delimiter, &quotes, &sup.FirstRow, &sup.BrandCol, &sup.ArticulCol, &sup.NameCol, &sup.PartnumCol, &sup.PriceCol, &sup.QuantityCol, &sup.RestCol); err != nil {
				return nil, err
			}
			if quotes {
				sup.Quotes = 1
			}
			if _, ok := res[sup.Name]; ok {
				return nil, errors.New("duplication of sups names :" + sup.Name)
			}
			res[sup.Name] = sup
		}
	}
	return res, nil
}

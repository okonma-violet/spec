package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx"
)

type repo struct {
	db *pgx.Conn
	//unsortedProductsByArt map[string]*[]*product
	//sortedProducts        []*product
	//categoriesByKeyword   map[string]*category
}

type category struct {
	id       int
	name     string
	keywords []string
}

type product struct {
	id         int
	categoryid int
	supplierid int // NOT WORKS IN TEST
	name       string
	articul    string
	partnum    string
	brand      string
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
	DROP TABLE IF EXISTS products,articuls,unsorted_products,categories CASCADE;
	DROP SEQUENCE IF EXISTS unsorted_products;
	CREATE TABLE "categories" (
		"id" SERIAL NOT NULL PRIMARY KEY,
		"name" TEXT NOT NULL,
		"keywords" TEXT ARRAY,
		UNIQUE("keywords")
	);
	`

	query += `
	CREATE TABLE "articuls" (
		"articul" TEXT NOT NULL PRIMARY KEY
	);
    `

	query += `
	CREATE TABLE "products" (
		"id"	SERIAL NOT NULL PRIMARY KEY,
		"categotyid" INTEGER NOT NULL,
		"supplierid" INTEGER NOT NULL, 
		"brand" TEXT NOT NULL,
		"articul"	TEXT NOT NULL,
		"name"	TEXT NOT NULL,
		"partnum"	TEXT,
		FOREIGN KEY("categotyid") REFERENCES "categories"("id"),
		FOREIGN KEY("articul") REFERENCES "articuls"("articul")
	);
    `
	query += `
	CREATE TABLE "unsorted_products" (
		"id" SERIAL NOT NULL PRIMARY KEY,
		"supplierid" INTEGER NOT NULL,
		"name" TEXT NOT NULL,
		"brand" TEXT NOT NULL,
		"articul" TEXT NOT NULL,
		"partnum" TEXT,
		FOREIGN KEY("articul") REFERENCES "articuls"("articul"),
		UNIQUE("supplierid","articul")
	);
	`
	_, err := rep.db.Exec(context.Background(), query)
	return err
}

var ErrDuplicate = errors.New("duplicates")

func (r *repo) CreateProduct(articul, name, partnum, brand string, categoryid int) (*product, error) {
	id := 0
	articul = normstring(articul)
	if err := r.db.QueryRow(context.Background(), "INSERT INTO products(articul,categoryid,name,partnum,brand) values($1,$2,$3,$4) RETURNING id", articul, categoryid, name, partnum, brand).Scan(&id); err != nil {
		return nil, err
	}
	return &product{
		id:      id,
		articul: articul,
		name:    name,
		partnum: partnum,
		brand:   brand,
	}, nil
}

func (r *repo) DeleteUnsortedProduct(id int) error {
	_, err := r.db.Exec(context.Background(), "DELETE FROM unsorted_products WHERE id=($1)", id)
	return err

}

func (r *repo) CreateUnsortedProduct(name, brand, articul, partnum string, supplierid int) (int, error) {
	id := 0
	if err := r.db.QueryRow(context.Background(), "INSERT INTO unsorted_products(name,brand,articul,partnum,supplierid) values($1,$2,$3,$4,$5) RETURNING id", name, brand, articul, partnum, supplierid).Scan(&id); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return 0, ErrDuplicate
		}
		return 0, err
	}
	return id, nil
}

func (r *repo) CreateArticul(articul string) error {
	if _, err := r.db.Exec(context.Background(), "INSERT INTO articuls(articul) values($1)", articul); err != nil {
		return err
	}
	return nil
}

func (r *repo) GetArticules() ([]string, error) {
	rows, err := r.db.Query(context.Background(), "SELECT articul FROM articuls")
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	for rows.Next() {
		var art string
		if err = rows.Scan(&art); err != nil {
			return nil, err
		}
		result = append(result, art)
	}
	return result, nil
}

func (r *repo) GetUnsortedProductsByArticul(articul string) ([]product, error) {
	rows, err := r.db.Query(context.Background(), fmt.Sprintf("SELECT id,name,articul,partnum,brand,supplierid FROM unsorted_products WHERE articul='%s'", articul))
	if err != nil {
		return nil, err
	}

	result := make([]product, 0)
	for rows.Next() {
		var data product
		if err = rows.Scan(&data.id, &data.name, &data.articul, &data.partnum, &data.brand, &data.supplierid); err != nil {
			return nil, err
		}
		result = append(result, data)
	}

	return result, nil
}

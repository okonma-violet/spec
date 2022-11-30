package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx"
)

var ErrDuplicate = errors.New("duplicates")

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
	supplierid int
	brandid    int
	name       string
	articul    string
	partnum    string
	price      float32
	quantity   int
	rest       int
}

type brand struct {
	id   int
	name string
	norm []string
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
	DROP TABLE IF EXISTS products,articuls,unsorted_products,categories,categories_keywords,suppliers,brands CASCADE;
	DROP SEQUENCE IF EXISTS products_unique_id;
	`
	query += `
	CREATE TABLE "brands" (
		"id"	SERIAL NOT NULL PRIMARY KEY,
		"name"	TEXT NOT NULL,
		"norm"  TEXT [] NOT NULL,
		UNIQUE("name")
	);
    `
	query += `
	CREATE TABLE "categories" (
		"id"	SERIAL NOT NULL PRIMARY KEY,
		"name"	TEXT NOT NULL,
		UNIQUE("name")
	);
    `
	query += `
	CREATE TABLE "categories_keywords" (
		"keyword" TEXT NOT NULL,
		"categoryid" INTEGER,
		UNIQUE("keyword"),
		FOREIGN KEY("categoryid") REFERENCES "categories"("id")
	);
    `
	query += `
	CREATE TABLE "suppliers" (
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
		"brandid" INTEGER NOT NULL, 
		"articul" TEXT NOT NULL,
		"name"	TEXT NOT NULL,
		"partnum"	TEXT,
		"price" REAL NOT NULL,
		"quantity" INTEGER,
		"rest" INTEGER,
		"updated" TIMESTAMP NOT NULL,
		FOREIGN KEY("categotyid") REFERENCES "categories"("id"),
		FOREIGN KEY("articul") REFERENCES "articuls"("articul"),
		FOREIGN KEY("supplierid") REFERENCES "suppliers"("id")
	);
    `

	query += `
	CREATE TABLE "unsorted_products" (
		"id" SERIAL NOT NULL PRIMARY KEY,
		"name" TEXT NOT NULL,
		"brandid" INTEGER NOT NULL, 
		"supplierid" INTEGER NOT NULL, 
		"articul" TEXT NOT NULL,
		"partnum" TEXT,
		"price" REAL NOT NULL,
		"quantity" INTEGER,
		"rest" INTEGER,
		"updated" TIMESTAMP NOT NULL DEFAULT current_timestamp,
		UNIQUE("supplierid","brandid","articul"),
		FOREIGN KEY("articul") REFERENCES "articuls"("articul"),
		FOREIGN KEY("supplierid") REFERENCES "suppliers"("id"),
		FOREIGN KEY("brandid") REFERENCES "brands"("id")
	);
	`
	query += `
	CREATE UNIQUE INDEX products_unique_id ON products (supplierid,brandid,articul);
	`
	_, err := rep.db.Exec(context.Background(), query)
	return err
}

func (r *repo) CreateProduct(articul string, categoryid, supplierid, brandid int, name string, price float32, partnum string, quantity, rest int) (*product, error) {
	id := 0
	articul = normstring(articul)
	if err := r.db.QueryRow(context.Background(), "INSERT INTO products(articul,categoryid,supplierid,brandid,name,price,partnum,quantity,rest) values($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id", articul, categoryid, supplierid, brandid, name, price, partnum, quantity, rest).Scan(&id); err != nil {
		var pgErr *pgconn.PgError // TODO: обновление данных
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return nil, ErrDuplicate
		}
		return nil, err
	}
	return &product{
		id:         id,
		categoryid: categoryid,
		supplierid: supplierid,
		brandid:    brandid,
		name:       name,
		price:      price,
		partnum:    partnum,
		quantity:   quantity,
		rest:       rest,
	}, nil
}

func (r *repo) DeleteUnsortedProduct(id int) error {
	_, err := r.db.Exec(context.Background(), "DELETE FROM unsorted_products WHERE id=($1)", id)
	return err

}

func (r *repo) CreateUnsortedProduct(articul string, supplierid, brandid int, name string, price float32, partnum string, quantity, rest int) (int, error) {
	id := 0
	if err := r.db.QueryRow(context.Background(), "INSERT INTO unsorted_products(articul,supplierid,brandid,name,price,partnum,quantity,rest) values($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id", articul, supplierid, brandid, name, price, partnum, quantity, rest).Scan(&id); err != nil {
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
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return ErrDuplicate
		}
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
	rows, err := r.db.Query(context.Background(), fmt.Sprintf("SELECT id,articul,supplierid,brandid,name,price,partnum,quantity,rest FROM unsorted_products WHERE articul='%s'", articul))
	if err != nil {
		return nil, err
	}

	result := make([]product, 0)
	for rows.Next() {
		var data product
		if err = rows.Scan(&data.id, &data.articul, &data.supplierid, &data.brandid, &data.name, &data.price, &data.partnum, &data.quantity, &data.rest); err != nil {
			return nil, err
		}
		result = append(result, data)
	}
	return result, nil
}

func (r *repo) CreateBrand(name string, norm []string) error {
	if _, err := r.db.Exec(context.Background(), "INSERT INTO brands(name,norm) values($1,$2)", name, norm); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return ErrDuplicate
		}
		return err
	}
	return nil
}

func (r *repo) GetBrandIdByNorm(norm string) (int, error) {
	var id int
	if err := r.db.QueryRow(context.Background(), "SELECT id FROM brands WHERE norm@>ARRAY[$1]", norm).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
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

func (r *repo) CreateSupplier(name, email, filename, delimiter string, quotes bool, firstrow, brandcol, articulcol int, namecol []int, partnumcol, pricecol, quantitycol, restcol int) (int, error) {
	id := 0
	if err := r.db.QueryRow(context.Background(), "INSERT INTO suppliers(name,email,filename,delimiter,quotes,firstrow,brandcol,articulcol,namecol,partnumcol,pricecol,quantitycol,restcol) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING id", name, strings.ToLower(email), strings.ToLower(filename), delimiter, quotes, firstrow, brandcol, articulcol, namecol, partnumcol, pricecol, quantitycol, restcol).Scan(&id); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return 0, ErrDuplicate
		}
		return 0, err
	}
	return id, nil
}

func (r *repo) GetSupplierByFilename(filename string) (*supplier, error) {
	sup := supplier{}
	var quotes bool
	if err := r.db.QueryRow(context.Background(), "SELECT id,name,email,filename,delimiter,quotes,firstrow,brandcol,articulcol,namecol,partnumcol,pricecol,quantitycol,restcol FROM suppliers WHERE filename=($1)", filename).Scan(
		&sup.id, &sup.Name, &sup.Email, &sup.Filename, &sup.Delimiter, &quotes, &sup.FirstRow, &sup.BrandCol, &sup.ArticulCol, &sup.NameCol, &sup.PartnumCol, &sup.PriceCol, &sup.QuantityCol, &sup.RestCol); err != nil {
		return nil, err
	}
	if quotes {
		sup.Quotes = 1
	}
	return &sup, nil
}

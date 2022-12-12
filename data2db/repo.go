package main

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgconn"
)

var ErrDuplicate = errors.New("duplicates")
var ErrNotExists = errors.New("not found")

type repo struct {
	db *pgx.Conn
	//unsortedProductsByArt map[string]*[]*product
	//sortedProducts        []*product
	//categoriesByKeyword   map[string]*category
}

type category struct {
	id   int
	name string
	norm string
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
	DROP TABLE IF EXISTS products,articuls,unsorted_products,categories,categories_keyphrases,products_keywords,suppliers,brands,prices CASCADE;
	DROP SEQUENCE IF EXISTS products_unique_id,unsorted_products_unique_id;
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
		"norm" TEXT NOT NULL,
		UNIQUE("name"),
		UNIQUE("norm")
	);
    `

	query += `
	CREATE TABLE "categories_keyphrases" (
		"id"	SERIAL NOT NULL PRIMARY KEY,
		"keyphrase"	TEXT NOT NULL,
		"categoryid" INTEGER NOT NULL,
		UNIQUE("keyphrase"),
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
		"articul" TEXT NOT NULL,
		"brandid" INTEGER NOT NULL,
		"categoryid" INTEGER,
		UNIQUE("articul","brandid"),
		FOREIGN KEY("categoryid") REFERENCES "categories"("id"),
		FOREIGN KEY("brandid") REFERENCES "brands"("id")
	);
    `

	query += `
	CREATE TABLE "products" (
		"id"	SERIAL NOT NULL PRIMARY KEY,
		"supplierid" INTEGER NOT NULL, 
		"brandid" INTEGER NOT NULL, 
		"articul" TEXT NOT NULL,
		"name"	TEXT NOT NULL,
		"partnum"	TEXT,
		"quantity" INTEGER,
		"hash" TEXT NOT NULL,
		UNIQUE("hash"),
		FOREIGN KEY("articul","brandid") REFERENCES "articuls"("articul","brandid"),
		FOREIGN KEY("supplierid") REFERENCES "suppliers"("id")
	);
    `

	query += `
	CREATE TABLE "prices" (
		"productid" INTEGER NOT NULL,
		"price" REAL NOT NULL,
		"rest" INTEGER,
		"updated" TIMESTAMP NOT NULL DEFAULT current_timestamp,
		UNIQUE("productid"),
		FOREIGN KEY("productid") REFERENCES "products"("id")
	);
    `

	query += `
	CREATE UNIQUE INDEX products_unique_id ON products (supplierid,name,brandid,articul);
	`
	_, err := rep.db.Exec(context.Background(), query)
	return err
}

func (r *repo) AddCategory(name, normname string) (int, error) {
	id := 0
	if err := r.db.QueryRow(context.Background(), "INSERT INTO categories(name,norm) values($1,$2) RETURNING id", name, normname).Scan(&id); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return 0, ErrDuplicate
		}
		return 0, err
	}
	return id, nil
}

func (r *repo) GetCategoryIdByNorm(norm string) (int, error) {
	var id int
	if err := r.db.QueryRow(context.Background(), "SELECT id FROM categories WHERE norm=$1", norm).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrNotExists
		}
		return 0, err
	}
	return id, nil
}

func (r *repo) AddCategoryKeyphrase(categoryid int, keyphrase string) error {
	if _, err := r.db.Exec(context.Background(), "INSERT INTO categories_keyphrases(keyphrase,categoryid) values($1,$2)", keyphrase, categoryid); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return ErrDuplicate
		}
		return err
	}
	return nil
}

func (r *repo) GetCategoriesKeyphrases() ([]*keyPhrase, error) {
	rows, err := r.db.Query(context.Background(), "SELECT keyphrase,categoryid FROM categories_keyphrases")
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotExists
		}
		return nil, err
	}
	kps := make([]*keyPhrase, 0)
	for rows.Next() {
		var kp keyPhrase
		if err := rows.Scan(&kp.phrase, &kp.catid); err != nil {
			rows.Close()
			return kps, err
		}
		kps = append(kps, &kp)
	}
	return kps, nil
}

func (r *repo) GetOrCreateProduct(articul string, supplierid, brandid int, name string, partnum string, quantity int) (int, error) {
	hashstr, err := getProductMD5(brandid, articul, name)
	if err != nil {
		return 0, err
	}
	id := 0
	if err := r.db.QueryRow(context.Background(), "SELECT id FROM products where hash=$1", hashstr).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if err := r.db.QueryRow(context.Background(), `INSERT INTO products(articul,supplierid,brandid,name,partnum,quantity,hash)
			values($1,$2,$3,$4,$5,$6,$7)
			RETURNING id`, articul, supplierid, brandid, name, partnum, quantity, hashstr).Scan(&id); err != nil {
				return 0, err
			}
			return id, nil
		}
		return 0, err
	}
	return id, nil
}

func (r *repo) UpsertPrice(productid int, price float32, rest int) error {
	_, err := r.db.Exec(context.Background(), `INSERT INTO prices(productid,price,rest)
	values($1,$2,$3)
	ON CONFLICT (productid) 
	DO UPDATE SET price=EXCLUDED.price,rest=EXCLUDED.rest,updated=now()`, productid, price, rest)
	return err
}

func (r *repo) CreateArticul(articul string, brandid int) error {
	if _, err := r.db.Exec(context.Background(), "INSERT INTO articuls(articul,brandid,categoryid) values($1,$2,null)", articul, brandid); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return ErrDuplicate
		}
		return err
	}
	return nil
}
func (r *repo) UpdateArticulCategory(articul string, brandid, categoryid int) error {
	ct, err := r.db.Exec(context.Background(), "UPDATE articuls SET categoryid = $1 WHERE articul = $2 AND brandid = $3", categoryid, articul, brandid)
	if err != nil {
		return err
	}
	if ct.String() == "UPDATE 0" {
		return ErrNotExists
	}
	return nil
}

type articulrow struct {
	articul    string
	brandid    int
	categoryid int
}

func (r *repo) GetUncategorizedArticulesWithBrandids() ([]*articulrow, error) {
	rows, err := r.db.Query(context.Background(), "SELECT articul,brandid FROM articuls WHERE categoryid is null")
	if err != nil {
		return nil, err
	}
	result := make([]*articulrow, 0)
	for rows.Next() {
		var artrow articulrow
		if err = rows.Scan(&artrow.articul, &artrow.brandid); err != nil {
			rows.Close()
			return result, err
		}
		result = append(result, &artrow)
	}
	return result, nil
}

func (r *repo) GetProductsNamesByArtAndBrand(articul string, brandid int) ([]string, error) {
	rows, err := r.db.Query(context.Background(), "SELECT name FROM products WHERE articul=$1 AND brandid=$2", articul, brandid)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	for rows.Next() {
		var pname string
		if err = rows.Scan(&pname); err != nil {
			rows.Close()
			return result, err
		}
		result = append(result, pname)
	}
	return result, nil
}

func (r *repo) CreateBrand(name string, norm []string) (int, error) {
	var id int
	if err := r.db.QueryRow(context.Background(), "INSERT INTO brands(name,norm) values($1,$2) RETURNING id", name, norm).Scan(&id); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return 0, ErrDuplicate
		}
		return 0, err
	}
	return id, nil
}

func (r *repo) GetBrandIdByNorm(norm string) (int, error) {
	var id int
	if err := r.db.QueryRow(context.Background(), "SELECT id FROM brands WHERE $1 = ANY (norm)", norm).Scan(&id); err != nil { //norm@>ARRAY[$1]
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrNotExists
		}
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
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotExists
		}
		return nil, err
	}
	if quotes {
		sup.Quotes = 1
	}
	return &sup, nil
}
func getProductMD5(brandid int, articul, name string) (string, error) {
	hash := md5.New()
	b := make([]byte, 4+len(articul)+len(name))
	binary.LittleEndian.PutUint32(b, uint32(brandid))
	copy(b[4:], []byte(articul))
	copy(b[4+len(articul):], []byte(name))
	if _, err := hash.Write(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

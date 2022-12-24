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

// TODO: яебу как уникальность additional_articul в articuls обозначить

var ErrDuplicate = errors.New("duplicates")
var ErrNotExists = errors.New("not found")

type repo struct {
	db *pgx.Conn
}

func (r *repo) OpenDBRepository(connectionString string) (err error) {
	r.db, err = pgx.Connect(context.Background(), connectionString)
	return err
}

func (rep *repo) Migrate(droptables bool) error {
	var query string
	if droptables {
		query += `
		DROP TABLE IF EXISTS products,articuls,unsorted_products,categories,uploads,categories_keyphrases,products_keywords,suppliers,brands,prices_actual,prices_history CASCADE;
		DROP SEQUENCE IF EXISTS products_unique_id,unsorted_products_unique_id;
	`
	}

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
	CREATE TABLE "uploads" (
		"id" SERIAL NOT NULL PRIMARY KEY,
		"time"  TIMESTAMP NOT NULL DEFAULT current_timestamp
	);
    `
	query += `
	CREATE TABLE "suppliers" (
		"id"	SERIAL NOT NULL PRIMARY KEY,
		"name"	TEXT NOT NULL,
		"email"	TEXT NOT NULL,
		"filename"	TEXT NOT NULL,
		UNIQUE("filename"),
		UNIQUE("name")
	);
	`

	query += `
	CREATE TABLE "articuls" (
		"articul" TEXT NOT NULL,
		"additional_articul" TEXT ARRAY,
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
		"hash" CHAR(32) NOT NULL,
		UNIQUE("hash"),
		FOREIGN KEY("articul","brandid") REFERENCES "articuls"("articul","brandid"),
		FOREIGN KEY("supplierid") REFERENCES "suppliers"("id")
	);
    `

	query += `
	CREATE TABLE "prices_actual" (
		"productid" INTEGER NOT NULL,
		"price" REAL NOT NULL,
		"rest" INTEGER,
		"uploadid" INTEGER NOT NULL,
		UNIQUE("productid"),
		FOREIGN KEY("productid") REFERENCES "products"("id"),
		FOREIGN KEY("uploadid") REFERENCES "uploads"("id")
	);
    `
	query += `
	CREATE TABLE "prices_history" (
		"productid" INTEGER NOT NULL,
		"price" REAL NOT NULL,
		"rest" INTEGER,
		"uploadid" INTEGER NOT NULL,
		FOREIGN KEY("productid") REFERENCES "products"("id"),
		FOREIGN KEY("uploadid") REFERENCES "uploads"("id")
	);
    `

	query += `
	CREATE UNIQUE INDEX products_unique_id ON products (supplierid,name,brandid,articul);
	`
	_, err := rep.db.Exec(context.Background(), query)
	return err
}

func (r *repo) CreateUpload() (int, error) {
	id := 0
	if err := r.db.QueryRow(context.Background(), "INSERT INTO uploads(time) values(now()) RETURNING id").Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
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

func (r *repo) UpsertActualPrice(productid, uploadid int, price float32, rest int) error {
	_, err := r.db.Exec(context.Background(), `INSERT INTO prices_actual(productid,uploadid,price,rest)
	values($1,$2,$3,$4)
	ON CONFLICT (productid) 
	DO UPDATE SET price=EXCLUDED.price,rest=EXCLUDED.rest,uploadid=EXCLUDED.uploadid`, productid, uploadid, price, rest)
	return err
}

func (r *repo) UpdateOutOfStock(supplierid, uploadid int) (int, error) {
	ct, err := r.db.Exec(context.Background(), `UPDATE prices_actual
	SET rest=0,uploadid=$2
	FROM (SELECT id FROM products WHERE supplierid=$1) AS subq
	WHERE prices_actual.productid=subq.id
	AND uploadid<$2`, supplierid, uploadid)
	return int(ct.RowsAffected()), err
}

func (r *repo) InsertHistoryPrice(productid, uploadid int, price float32, rest int) error {
	_, err := r.db.Exec(context.Background(), `INSERT INTO prices_history(productid,uploadid,price,rest)
	values($1,$2,$3,$4)`, productid, uploadid, price, rest)
	return err
}

// adds additional articules if not exists or ones not equal with given
func (r *repo) UpsertArticul(articul string, brandid int, additional_articuls []string) error {
	_, err := r.db.Exec(context.Background(), `INSERT INTO articuls(articul,additional_articul,brandid,categoryid)
	VALUES($1,$2,$3,null)
	ON CONFLICT (articul,brandid)
	DO UPDATE SET additional_articul=EXCLUDED.additional_articul
	WHERE articuls.additional_articul<>EXCLUDED.additional_articul`, articul, additional_articuls, brandid)

	return err
}

// appends nonexisting additional_articuls
func (r *repo) UpsertArticul_NoAdditionalArticulesRewriting(articul string, brandid int, additional_articuls []string) error {
	_, err := r.db.Exec(context.Background(), `INSERT INTO articuls(articul,additional_articul,brandid,categoryid)
	VALUES($1,$2,$3,null)
	ON CONFLICT (articul,brandid)
	DO UPDATE SET additional_articul= (select array_agg(distinct e) from unnest(additional_articul || EXCLUDED.additional_articul) e)
	WHERE NOT additional_articul @> EXCLUDED.additional_articul`, articul, additional_articuls, brandid)

	return err
}
func (r *repo) UpdateArticulCategory(articul string, brandid, categoryid int) error {
	ct, err := r.db.Exec(context.Background(), "UPDATE articuls SET categoryid = $1 WHERE articul = $2 AND brandid = $3", categoryid, articul, brandid)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
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

func (r *repo) GetBrandIdByNorm(norm string) (int, []string, error) {
	var id int
	var norms []string
	if err := r.db.QueryRow(context.Background(), "SELECT id,norm FROM brands WHERE $1 = ANY (norm)", norm).Scan(&id, &norms); err != nil { //norm@>ARRAY[$1]
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil, ErrNotExists
		}
		return 0, nil, err
	}
	return id, norms, nil
}

type supplier struct {
	id       int
	Name     string
	Email    string
	Filename string
}

func (r *repo) CreateSupplier(name, email, filename string) (int, error) {
	id := 0
	if err := r.db.QueryRow(context.Background(), "INSERT INTO suppliers(name,email,filename) values($1,$2,$3) RETURNING id", name, strings.ToLower(email), strings.ToLower(filename)).Scan(&id); err != nil {
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
	if err := r.db.QueryRow(context.Background(), "SELECT id,name,email,filename FROM suppliers WHERE filename=($1)", filename).Scan(
		&sup.id, &sup.Name, &sup.Email, &sup.Filename); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotExists
		}
		return nil, err
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

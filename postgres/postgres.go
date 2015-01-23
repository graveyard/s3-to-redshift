package postgres

import (
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"

	"github.com/clever/pathio"
	"github.com/facebookgo/errgroup"
	_ "github.com/lib/pq"
	"gopkg.in/pg.v2"
)

var (
	// TODO: include flag validation
	host   = flag.String("postgreshost", "", "Address of the postgres host")
	port   = flag.Int("postgresport", 0, "Port to connect to")
	user   = flag.String("postgresuser", "", "Username for postgres")
	pwd    = flag.String("postgrespassword", "", "Password for the postgres user")
	dbname = flag.String("postgresdatabase", "", "Postgres database to connect to")
)

const schemaQueryFormat = `SELECT
  f.attnum as ordinal,
  f.attname AS name,
  pg_catalog.format_type(f.atttypid,f.atttypmod) AS col_type,
  CASE
      WHEN f.atthasdef = 't' THEN d.adsrc
      ELSE ''
  END AS default_val,
  f.attnotnull AS not_null,
  p.contype IS NOT NULL AND p.contype = 'p' AS primary_key
FROM pg_attribute f
  JOIN pg_class c ON c.oid = f.attrelid
  LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
  LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)
WHERE c.relkind = 'r'::char
    AND n.nspname = '%s'  -- Replace with Schema name
    AND c.relname = '%s'  -- Replace with table name
    AND f.attnum > 0 ORDER BY f.attnum`

type DB struct {
	*pg.DB
}

func NewDB() *DB {
	flag.Parse()
	opt := pg.Options{
		Host:     *host,
		Port:     fmt.Sprintf("%d", *port),
		User:     *user,
		Password: *pwd,
		Database: *dbname,
		SSL:      true,
	}
	return &DB{pg.Connect(&opt)}
}

type ColInfo struct {
	Ordinal    int
	Name       string
	ColType    string
	DefaultVal string
	NotNull    bool
	PrimaryKey bool
}

type TableSchema []*ColInfo

func (ts *TableSchema) New() interface{} {
	ci := &ColInfo{}
	*ts = append(*ts, ci)
	return ci
}

func (ts TableSchema) Len() int {
	return len(ts)
}

func (ts TableSchema) Less(i, j int) bool {
	return ts[i].Ordinal < ts[j].Ordinal
}

func (ts TableSchema) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}

type nopCloserBuffer struct {
	*bytes.Buffer
}

func (nopCloserBuffer) Close() error { return nil }

func S3Filename(prefix string, table string) string {
	return prefix + table + ".txt.gz"
}

func (db *DB) DumpTableToWriter(table string, w io.WriteCloser, format string, delim rune) error {
	cmd := fmt.Sprintf("COPY %s TO STDOUT WITH (FORMAT %s, DELIMITER '%c', HEADER 0)", table, format, delim)
	log.Print(cmd)
	_, err := db.CopyTo(w, cmd)
	return err
}

// TODO: include foreign key relations
func (db *DB) GetTableSchema(table string, namespace string) (TableSchema, error) {
	if namespace == "" {
		namespace = "public"
	}
	query := fmt.Sprintf(schemaQueryFormat, namespace, table)
	log.Print(query)
	var schema TableSchema
	if _, err := db.Query(&schema, query); err != nil {
		return nil, err
	}
	return schema, nil
}

func (db *DB) GetTableSchemas(tables []string, namespace string) (map[string]TableSchema, error) {
	group := new(errgroup.Group)
	tsmap := map[string]TableSchema{}
	for _, table := range tables {
		group.Add(1)
		go func(table string) {
			ts, err := db.GetTableSchema(table, namespace)
			if err != nil {
				group.Error(err)
			}
			tsmap[table] = ts
			group.Done()
		}(table)
	}
	err := group.Wait()
	return tsmap, err
}

func (db *DB) DumpTableToS3(table string, s3file string) error {
	buf := nopCloserBuffer{new(bytes.Buffer)}
	if err := db.DumpTableToWriter(table, gzip.NewWriter(buf), "csv", '|'); err != nil {
		return err
	}
	return pathio.WriteReader(s3file, buf, int64(buf.Len()))
}

func (db *DB) DumpTablesToS3(tables []string, s3_prefix string) error {
	group := new(errgroup.Group)
	for _, table := range tables {
		group.Add(1)
		go func(table string) {
			if err := db.DumpTableToS3(table, S3Filename(s3_prefix, table)); err != nil {
				group.Error(err)
			}
			group.Done()
		}(table)
	}
	return group.Wait()
}

package sqlitify

import (
	_ "github.com/mattn/go-sqlite3"
	"github.com/naoina/genmai"
)

func GetDB(dsn string) (db *genmai.DB, err error) {
	dialect := &genmai.SQLite3Dialect{}
	db, err = genmai.New(dialect, dsn)
	return
}

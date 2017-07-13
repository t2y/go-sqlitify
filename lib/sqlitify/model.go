package sqlitify

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"github.com/naoina/genmai"
	"github.com/pkg/errors"
)

type ExtDB struct {
	*genmai.DB

	db   *sql.DB
	name string
}

func (ext *ExtDB) AttachDatabase(path string) (err error) {
	var stmt *sql.Stmt
	if stmt, err = ext.db.Prepare("attach database ? as ?"); err != nil {
		err = errors.Wrap(err, "faild to prepare attach database")
		return
	}
	defer stmt.Close()

	if _, err = stmt.Exec(path, ext.name); err != nil {
		err = errors.Wrap(err, "faild to exec attach database")
		return
	}

	return
}

func (ext *ExtDB) DetachDatabase() (err error) {
	var stmt *sql.Stmt
	if stmt, err = ext.db.Prepare("detach database ?"); err != nil {
		err = errors.Wrap(err, "faild to prepare detach database")
		return
	}
	defer stmt.Close()

	if _, err = stmt.Exec(ext.name); err != nil {
		err = errors.Wrap(err, "faild to exec detach database")
		return
	}

	return
}

func (ext *ExtDB) SelectInsert(tables []genmai.TableNamer) (err error) {
	for _, table := range tables {
		tableName := table.TableName()
		query := fmt.Sprintf(
			"insert into %s select * from %s.%s",
			tableName, ext.name, tableName,
		)
		if _, err = ext.db.Exec(query); err != nil {
			err = errors.Wrap(err, "faild to exec select insert")
			return
		}
	}

	return
}

func (ext *ExtDB) Merge(path string, tables []genmai.TableNamer) (err error) {
	if err = ext.AttachDatabase(path); err != nil {
		err = errors.Wrap(err, "faild to merge")
		return
	}

	if err = ext.SelectInsert(tables); err != nil {
		err = errors.Wrap(err, "faild to merge")
		return
	}

	if err = ext.DetachDatabase(); err != nil {
		err = errors.Wrap(err, "faild to merge")
		return
	}

	return
}

func NewExtDB(db *genmai.DB, name string) *ExtDB {
	return &ExtDB{db, db.DB(), name}
}

func GetDB(dsn string) (db *genmai.DB, err error) {
	dialect := &genmai.SQLite3Dialect{}
	db, err = genmai.New(dialect, dsn)
	return
}

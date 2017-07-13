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
	db *sql.DB
}

func (ext *ExtDB) AttachDatabase(path, schemaName string) (err error) {
	var stmt *sql.Stmt
	if stmt, err = ext.db.Prepare("attach database ? as ?"); err != nil {
		err = errors.Wrap(err, "faild to prepare attach database")
		return
	}
	defer stmt.Close()

	if _, err = stmt.Exec(path, schemaName); err != nil {
		err = errors.Wrap(err, "faild to exec attach database")
		return
	}

	return
}

func (ext *ExtDB) DetachDatabase(schemaName string) (err error) {
	var stmt *sql.Stmt
	if stmt, err = ext.db.Prepare("detach database ?"); err != nil {
		err = errors.Wrap(err, "faild to prepare detach database")
		return
	}
	defer stmt.Close()

	if _, err = stmt.Exec(schemaName); err != nil {
		err = errors.Wrap(err, "faild to exec detach database")
		return
	}

	return
}

func (ext *ExtDB) SelectInsert(
	tables []genmai.TableNamer, schemaName string,
) (err error) {
	for _, table := range tables {
		tableName := table.TableName()
		query := fmt.Sprintf(
			"insert into %s select * from %s.%s",
			tableName, schemaName, tableName,
		)
		if _, err = ext.db.Exec(query); err != nil {
			err = errors.Wrap(err, "faild to exec select insert")
			return
		}
	}

	return
}

func (ext *ExtDB) Merge(
	path, schemaName string, tables []genmai.TableNamer,
) (err error) {
	if err = ext.AttachDatabase(path, schemaName); err != nil {
		err = errors.Wrap(err, "faild to merge")
		return
	}

	if err = ext.SelectInsert(tables, schemaName); err != nil {
		err = errors.Wrap(err, "faild to merge")
		return
	}

	if err = ext.DetachDatabase(schemaName); err != nil {
		err = errors.Wrap(err, "faild to merge")
		return
	}

	return
}

func (ext *ExtDB) CreateTablesIfNotExists(tables []genmai.TableNamer) (err error) {
	for _, table := range tables {
		if err = ext.CreateTableIfNotExists(table); err != nil {
			err = errors.Wrap(err, "faild to create table if not exists")
			return
		}
	}

	return
}

func NewExtDB(dsn string) (extDB *ExtDB, err error) {
	var db *genmai.DB
	db, err = NewDB(dsn)
	extDB = &ExtDB{db, db.DB()}
	return
}

func NewDB(dsn string) (db *genmai.DB, err error) {
	dialect := &genmai.SQLite3Dialect{}
	db, err = genmai.New(dialect, dsn)
	return
}

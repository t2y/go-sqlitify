package sqlitify

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"github.com/naoina/genmai"
	"github.com/pkg/errors"
)

type BulkData struct {
	tables  []genmai.TableNamer
	dataMap map[string][]interface{}
	cap     int
}

func (b *BulkData) Append(datum genmai.TableNamer) {
	tableName := datum.TableName()
	if data, ok := b.dataMap[tableName]; ok {
		b.dataMap[tableName] = append(data, datum)
	}
}

func (b *BulkData) NeedInsert() (r bool) {
	for _, data := range b.dataMap {
		if len(data) > 0 {
			r = true
			return
		}
	}
	return
}

func (b *BulkData) Reset() {
	for key, data := range b.dataMap {
		b.dataMap[key] = data[:0]
	}
}

func (b *BulkData) Insert(db *ExtDB) (err error) {
	for _, data := range b.dataMap {
		if len(data) > 0 {
			if _, err = db.Insert(data); err != nil {
				err = errors.Wrap(err, "faild to bulk insert data")
				return
			}
		}
	}

	b.Reset()

	return
}

func InsertBulkData(
	db *ExtDB, bulkData *BulkData, i int,
) (err error) {
	err = bulkData.Insert(db)
	return
}

func NewBulkData(
	tables []genmai.TableNamer, cap int,
) (d *BulkData) {
	dataMap := make(map[string][]interface{}, len(tables))
	for _, table := range tables {
		tableName := table.TableName()
		dataMap[tableName] = make([]interface{}, 0, cap)
	}

	d = &BulkData{
		tables:  tables,
		dataMap: dataMap,
		cap:     cap,
	}
	return
}

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

func NewExtDBWithTables(
	dsn string, tables []genmai.TableNamer,
) (db *ExtDB, err error) {
	if db, err = NewExtDB(dsn); err != nil {
		err = errors.Wrap(err, "failed to get ext database")
		return
	}

	if err = db.CreateTablesIfNotExists(tables); err != nil {
		err = errors.Wrap(err, "failed to create table")
		db.Close()
		return
	}

	return
}

func newDB(dsn string) (db *genmai.DB, err error) {
	dialect := &genmai.SQLite3Dialect{}
	db, err = genmai.New(dialect, dsn)
	return
}

func NewExtDB(dsn string) (extDB *ExtDB, err error) {
	var db *genmai.DB
	db, err = newDB(dsn)
	extDB = &ExtDB{db, db.DB()}
	return
}

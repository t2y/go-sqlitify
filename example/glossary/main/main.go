package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	flags "github.com/jessevdk/go-flags"
	"github.com/naoina/genmai"

	"github.com/t2y/go-sqlitify/example/glossary"
	"github.com/t2y/go-sqlitify/lib/sqlitify"
)

var opts sqlitify.Options

func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
	})
	log.SetOutput(os.Stdout)
}

func insertData(db *genmai.DB) func([]byte) error {
	return func(bytes []byte) error {
		data := &glossary.Data{}
		if err := json.Unmarshal(bytes, data); err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("failed to unmarshal json")
			return nil
		}

		glossDef := data.Glossary.GlossDiv.GlossList.GlossEntry.GlossDef
		glossDefSK, _ := db.LastInsertId()
		glossDefModel := &glossary.GlossDefModel{
			SK:           glossDefSK,
			Para:         glossDef.Para,
			GlossSeeAlso: strings.Join(glossDef.GlossSeeAlso, ", "),
		}
		if _, err := db.Insert(glossDefModel); err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("failed to insert data")
			return nil
		}

		glossEntry := data.Glossary.GlossDiv.GlossList.GlossEntry
		glossEntrySK, _ := db.LastInsertId()
		glossEntryModel := &glossary.GlossEntryModel{
			SK:         glossEntrySK,
			ID:         glossEntry.ID,
			SortAs:     glossEntry.SortAs,
			GlossTerm:  glossEntry.GlossTerm,
			Acronym:    glossEntry.Acronym,
			Abbrev:     glossEntry.Abbrev,
			GlossSee:   glossEntry.GlossSee,
			GlossDefSK: glossDefModel.SK,
		}
		if _, err := db.Insert(glossEntryModel); err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("failed to insert data")
			return nil
		}

		glossListSK, _ := db.LastInsertId()
		glossListModel := &glossary.GlossListModel{
			SK:           glossListSK,
			GlossEntrySK: glossEntryModel.SK,
		}
		if _, err := db.Insert(glossListModel); err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("failed to insert data")
			return nil
		}

		glossDiv := data.Glossary.GlossDiv
		glossDivID, _ := db.LastInsertId()
		glossDivModel := &glossary.GlossDivModel{
			SK:          glossDivID,
			Title:       glossDiv.Title,
			GlossListSK: glossListModel.SK,
		}
		if _, err := db.Insert(glossDivModel); err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("failed to insert data")
			return nil
		}

		glossarySK, _ := db.LastInsertId()
		glossaryModel := &glossary.GlossaryModel{
			SK:         glossarySK,
			Title:      data.Glossary.Title,
			GlossDivSK: glossDivModel.SK,
		}
		if _, err := db.Insert(glossaryModel); err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("failed to insert data")
			return nil
		}

		dataSK, _ := db.LastInsertId()
		dataModel := &glossary.DataModel{
			SK:         dataSK,
			GlossarySK: glossaryModel.SK,
		}
		if _, err := db.Insert(dataModel); err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("failed to insert data")
			return nil
		}

		db.Commit()
		return nil
	}
}

func createTableIfNotExists(db *genmai.DB) (err error) {
	err = db.CreateTableIfNotExists(&glossary.DataModel{})
	if err != nil {
		return
	}
	err = db.CreateTableIfNotExists(&glossary.GlossaryModel{})
	if err != nil {
		return
	}
	err = db.CreateTableIfNotExists(&glossary.GlossDivModel{})
	if err != nil {
		return
	}
	err = db.CreateTableIfNotExists(&glossary.GlossListModel{})
	if err != nil {
		return
	}
	err = db.CreateTableIfNotExists(&glossary.GlossEntryModel{})
	if err != nil {
		return
	}
	err = db.CreateTableIfNotExists(&glossary.GlossDefModel{})
	if err != nil {
		return
	}

	return
}

func main() {
	if _, err := flags.Parse(&opts); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		}
		os.Exit(1)
	}

	if opts.Verbose {
		log.SetLevel(log.DebugLevel)
	}

	if err := sqlitify.InitOptions(&opts); err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to initialize options")
	}

	dsn := filepath.Join(opts.OutputPath, opts.DBName)

	db, err := sqlitify.NewDB(dsn)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to get database")
	}

	if err = createTableIfNotExists(db); err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to create table")
	}

	/*
		for _, path := range opts.Paths {
			sqlitify.ReadData(path, insertData(db))
		}
	*/
}

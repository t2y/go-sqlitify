package main

import (
	"os"
	"path/filepath"
	"time"

	log "github.com/Sirupsen/logrus"
	flags "github.com/jessevdk/go-flags"
	"github.com/pkg/errors"

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

func newReader(opts *sqlitify.Options) (r sqlitify.Reader) {
	r = sqlitify.NewReader(opts, sqlitify.TypeJsonReader)
	r.RegisterEachCallback(createData)
	r.RegisterIntervalCallback(sqlitify.InsertBulkData)
	return
}

func integrateData(opts *sqlitify.Options, r sqlitify.Reader) {
	var mergedPath string
	dataPaths := make([]string, 0, 1024)
	di := sqlitify.NewDataIntegrator(opts, sqlitify.TypeGroupIntegrator)
	for {
		paths, err := r.GetDBFiles()
		if err != nil {
			log.WithFields(log.Fields{
				"number of data files": len(paths),
				"err": err,
			}).Debug("finished to get data files from reader")
			break
		}

		log.WithFields(log.Fields{
			"number of data files": len(paths),
			"paths":                paths,
		}).Debug("got created data files")

		if mergedPath != "" {
			dataPaths = append(dataPaths, mergedPath)
		}

		dataPaths = append(dataPaths, paths...)
		mergedPath, err = di.Run(dataPaths, opts.Tables)
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Fatal("Failed to run data integration")
		}

		log.WithFields(log.Fields{
			"mergedPath": mergedPath,
		}).Info("successfully merged")

		dataPaths = dataPaths[:0]
	}

	resultPath := filepath.Join(opts.OutputPath, opts.DBName)
	if err := os.Rename(mergedPath, resultPath); err != nil {
		err = errors.Wrap(err, "failed to rename a file")
		return
	}

	db, err := sqlitify.NewExtDB(resultPath)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to get ext database")
	}

	log.Info("start creating indexes to merged data file")
	createIndexes(db)
	log.Info("end creating indexes to merged data file")

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

	log.Info("start sqlitify")

	if err := sqlitify.InitOptions(&opts); err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to initialize options")
	}
	// set tables from generated source code
	opts.Tables = tables

	r := newReader(&opts)
	if opts.WithoutIntegrate {
		r.Run()
		return
	} else {
		go r.Run()
	}

	integrateData(&opts, r)

	log.Info("end sqlitify")
}

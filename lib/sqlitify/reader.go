package sqlitify

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

const (
	maxNumberOfDataFiles = 31
	maxReadChannelSize   = 1024

	intervalReadLineMessage = 100000
)

const (
	TypeJsonReader = "json"
)

var (
	ErrNoDataFile = errors.New("no data file to get")
)

type Reader interface {
	RegisterEachCallback(func(*ExtDB, *BulkData, []byte) error)
	RegisterIntervalCallback(func(*ExtDB, *BulkData, int) error)

	Read(string, *ExtDB, *BulkData) error
	Run() error
	GetDBFile() (string, error)
	GetDBFiles() ([]string, error)

	IsFinished() bool
}

type JsonReader struct {
	opts   *Options
	readCh chan string

	eachCallback     func(*ExtDB, *BulkData, []byte) error
	intervalCallback func(*ExtDB, *BulkData, int) error

	mu              sync.Mutex
	readFinished    bool
	getDataFinished bool
}

func (r *JsonReader) RegisterEachCallback(f func(*ExtDB, *BulkData, []byte) error) {
	r.eachCallback = f
}

func (r *JsonReader) RegisterIntervalCallback(f func(*ExtDB, *BulkData, int) error) {
	r.intervalCallback = f
}

func (r *JsonReader) Read(path string, db *ExtDB, bulkData *BulkData) (err error) {
	log.WithFields(log.Fields{
		"path": path,
	}).Info("start reading file")

	var reader io.Reader

	var f *os.File
	f, err = os.Open(path)
	if err != nil {
		err = errors.Wrap(err, "faild to open")
		return
	}
	defer f.Close()
	reader = f

	if strings.HasSuffix(path, ".gz") {
		var g *gzip.Reader
		g, err = gzip.NewReader(f)
		if err != nil {
			err = errors.Wrap(err, "failed to create reader")
			return
		}
		defer g.Close()
		reader = g
	}

	i := 1
	scanner := bufio.NewScanner(reader)

	if r.opts.LineBufferSize != 0 && r.opts.LineBufferSize > bufio.MaxScanTokenSize {
		buf := make([]byte, bufio.MaxScanTokenSize, r.opts.LineBufferSize)
		scanner.Buffer(buf, r.opts.LineBufferSize)
		log.WithFields(log.Fields{
			"LineBufferSize": r.opts.LineBufferSize,
		}).Info("set line buffer")
	}

	for scanner.Scan() {
		if r.eachCallback != nil {
			if err = r.eachCallback(db, bulkData, scanner.Bytes()); err != nil {
				log.WithFields(log.Fields{
					"line": scanner.Text(),
					"err":  err,
				}).Error("failed to process eachCallback function")
				break
			}
		}

		if i%r.opts.NumOfBulkInsert == 0 {
			if r.intervalCallback != nil {
				if err = r.intervalCallback(db, bulkData, i); err != nil {
					log.WithFields(log.Fields{
						"line number": i,
						"err":         err,
					}).Error("failed to process intervalCallback function")
					break
				}
			}
		}

		if i%intervalReadLineMessage == 0 {
			log.WithFields(log.Fields{
				"line": i,
				"path": path,
			}).Debug("read lines")
		}

		i += 1
	}

	if err = scanner.Err(); err != nil {
		err = errors.Wrap(err, "failed to scan file")
		return
	}

	log.WithFields(log.Fields{
		"path": path,
	}).Info("end reading log file")
	return
}

func (r *JsonReader) Run() (err error) {
	var wg sync.WaitGroup
	pathCh := make(chan string, r.opts.Concurrent)

	for i := 0; i < int(r.opts.Concurrent); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				path, ok := <-pathCh
				if !ok {
					return
				}

				dsn := MakeDBPath(path, r.opts.OutputPath)
				db, err := NewExtDB(dsn)
				if err != nil {
					log.WithFields(log.Fields{
						"err": err,
					}).Error("Failed to get database")
					continue
				}

				if err = db.CreateTablesIfNotExists(r.opts.Tables); err != nil {
					db.Close()
					log.WithFields(log.Fields{
						"err": err,
					}).Error("Failed to create tables")
					continue
				}

				bulkData := NewBulkData(r.opts.Tables, r.opts.NumOfBulkInsert)
				if err = r.Read(path, db, bulkData); err != nil {
					db.Close()
					log.WithFields(log.Fields{
						"err": err,
					}).Error("Failed to read data")
					continue
				}

				if bulkData.NeedInsert() {
					if err := bulkData.Insert(db); err != nil {
						log.WithFields(log.Fields{
							"err": err,
						}).Error("Failed to insert data")
					}
				}

				db.Close()

				if len(r.readCh) == cap(r.readCh) {
					// block to send until channel would be cleared
					r.readCh <- dsn
				} else {
					select {
					case r.readCh <- dsn:
					default:
						log.WithFields(log.Fields{
							"dsn": dsn,
						}).Panic("Never reach here")
					}
				}
			}
		}()
	}

	for _, path := range r.opts.InputPaths {
		pathCh <- path
	}
	close(pathCh)

	wg.Wait()

	r.mu.Lock()
	r.readFinished = true
	r.mu.Unlock()

	log.Info("finished reading data and inserted each sqlite.db")
	return
}

// ensure to returns path or error
func (r *JsonReader) GetDBFile() (path string, err error) {
	var ok bool
	for {
		select {
		case path, ok = <-r.readCh:
			if !ok {
				r.mu.Lock()
				r.getDataFinished = true
				r.mu.Unlock()
				err = ErrNoDataFile
				log.Info("finished to get data file from channel")
			}

			return
		default:
			if r.readFinished {
				if len(r.readCh) == 0 {
					close(r.readCh)
				}
			}

			time.Sleep(1 * time.Second)
		}
	}
}

func (r *JsonReader) GetDBFiles() (paths []string, err error) {
	if r.getDataFinished {
		err = ErrNoDataFile
		return
	}

	paths = make([]string, 0, r.opts.NumOfDBFiles)
	for {
		path, e := r.GetDBFile()
		if e != nil {
			log.WithFields(log.Fields{
				"e": e,
			}).Debug("closed read channel")
			if len(paths) == 0 {
				err = e
			}
			return
		}

		paths = append(paths, path)
		if len(paths) == r.opts.NumOfDBFiles {
			return
		}
	}
}

func (r *JsonReader) IsFinished() bool {
	return r.readFinished && r.getDataFinished
}

func NewJsonReader(opts *Options) (r *JsonReader) {
	r = &JsonReader{
		opts:   opts,
		readCh: make(chan string, opts.NumOfDBFiles),
	}
	return
}

func NewReader(opts *Options, typ string) (r Reader) {
	switch typ {
	case TypeJsonReader:
		r = NewJsonReader(opts)
	}
	return
}

package sqlitify

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

type Options struct {
	Concurrent uint   `long:"concurrent" default:"2" description:"number of concurrent reading data files"`
	InputPath  string `long:"inputPath" required:"true" description:"path to input data file"`
	DBPath     string `long:"dbPath" description:"path to sqlite file"`
	Command    string `long:"command" description:"init or ..."`

	ArgSince string `long:"since" description:"filter since date"`
	ArgUntil string `long:"until" description:"filter until date"`

	Verbose bool `long:"verbose" description:"use verbose mode"`

	// internal use
	Paths []string
	Since *time.Time
	Until *time.Time

	mu      sync.Mutex
	DBPaths []string
}

func (o *Options) AppendDBPath(path string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.DBPaths = append(o.DBPaths, path)
}

func parseDateTimeArgument(dateStr string) (t *time.Time, err error) {
	parsed, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		err = errors.Wrap(err, "failed to parse")
		return
	}
	t = &parsed
	return
}

func InitOptions(opts *Options) (err error) {
	if opts.DBPath == "" {
		opts.DBPath = "test-sqlite.db"
	}

	if opts.ArgSince != "" {
		opts.Since, err = parseDateTimeArgument(opts.ArgSince)
		if err != nil {
			err = errors.Wrap(err, "failed to parse since date")
			return
		}
	}

	if opts.ArgUntil != "" {
		opts.Until, err = parseDateTimeArgument(opts.ArgUntil)
		if err != nil {
			err = errors.Wrap(err, "failed to parse until date")
			return
		}
	}

	opts.Paths, err = Walk(opts)
	if len(opts.Paths) == 0 {
		err = errors.Errorf("no files in the directory: %s", opts.InputPath)
		return
	}

	log.WithFields(log.Fields{
		"opts": opts,
	}).Debug("init options")
	return
}

func Walk(opts *Options) (paths []string, err error) {
	var info os.FileInfo
	info, err = os.Stat(opts.InputPath)
	if os.IsNotExist(err) {
		err = errors.Wrap(err, "not found input path")
		return
	}

	if !info.IsDir() {
		paths = append(paths, opts.InputPath)
		return
	}

	err = filepath.Walk(opts.InputPath,
		func(path string, f os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if f.IsDir() {
				return nil
			}

			log.WithFields(log.Fields{
				"name": f.Name(),
			}).Debug("walking path")

			if opts.Since != nil && f.ModTime().Before(*opts.Since) {
				return nil
			}
			if opts.Until != nil && f.ModTime().After(*opts.Until) {
				return nil
			}

			paths = append(paths, path)
			return nil
		})
	return
}

package sqlitify

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

func ReadData(
	path string,
	callbackEach func([]byte) error,
	interval int,
	callbackInterval func(int) error,
) (err error) {
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
		var r *gzip.Reader
		r, err = gzip.NewReader(f)
		if err != nil {
			err = errors.Wrap(err, "failed to create reader")
			return
		}
		defer r.Close()
		reader = r
	}

	i := 1
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		if err = callbackEach(scanner.Bytes()); err != nil {
			log.WithFields(log.Fields{
				"line": scanner.Text(),
				"err":  err,
			}).Error("failed to process callbackEach function")
			break
		}

		if i%interval == 0 {
			if err = callbackInterval(i); err != nil {
				log.WithFields(log.Fields{
					"line number": i,
					"err":         err,
				}).Error("failed to process callbackInterval function")
				break
			}
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

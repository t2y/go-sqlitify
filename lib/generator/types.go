package generator

import (
	"encoding/json"
	"io"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

type TypesCode struct {
	fileName string
	source   string
}

func (t *TypesCode) GetFileName() string {
	return t.fileName
}

func (t *TypesCode) GetSource() string {
	return t.source
}

func (t *TypesCode) Parse(r io.Reader) (err error) {
	var data interface{}
	if err = json.NewDecoder(r).Decode(&data); err != nil {
		err = errors.Wrap(err, "failed to parse as json")
		return
	}

	log.Debug(data)

	return
}

func NewTypesCode(path string) (code *TypesCode) {
	code = &TypesCode{
		fileName: GenerateFileName(path),
	}
	return
}

func GenerateTypes(path string) (code *TypesCode, err error) {
	f, err := os.Open(path)
	if err != nil {
		err = errors.Wrap(err, "failed to open")
		return
	}
	defer f.Close()

	code = NewTypesCode(path)
	if err = code.Parse(f); err != nil {
		err = errors.Wrap(err, "failed to parse")
		return
	}
	return
}

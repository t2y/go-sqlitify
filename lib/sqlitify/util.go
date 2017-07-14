package sqlitify

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"

	uuid "github.com/nu7hatch/gouuid"
	"github.com/pkg/errors"
)

func GetUUID() string {
	_uuid, _ := uuid.NewV4()
	return _uuid.String()
}

func MakeDBPath(inputPath, outputDir string) (dbPath string) {
	basename := filepath.Base(inputPath)
	var name string
	if strings.Contains(basename, ".") {
		name = strings.Replace(basename, filepath.Ext(basename), "", 1)
	} else {
		name = basename + "-" + GetUUID()
	}
	dbPath = filepath.Join(outputDir, name+".db")
	return
}

func MakeDBPathFromGroup(
	pair []string, outputDir string,
) (dbPath string) {
	names := make([]string, 0, len(pair))
	for _, path := range pair {
		basename := filepath.Base(path)
		name := strings.Replace(basename, filepath.Ext(basename), "", 1)
		names = append(names, name)
	}

	concatenated := strings.Join(names, "+")
	dbPath = filepath.Join(outputDir, concatenated+".db")
	return
}

func RemoveIfExists(path string) (err error) {
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		if err = os.Remove(path); err != nil {
			err = errors.Wrap(err, "failed to remove a file")
			return
		}
	}

	return
}

func GetTypeName(t interface{}) (name string) {
	typ := reflect.TypeOf(t)
	if typ.Kind() == reflect.Ptr {
		name = typ.Elem().Name()
	} else {
		name = typ.Name()
	}

	return
}

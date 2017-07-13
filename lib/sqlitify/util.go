package sqlitify

import (
	"path/filepath"
	"strings"

	uuid "github.com/nu7hatch/gouuid"
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

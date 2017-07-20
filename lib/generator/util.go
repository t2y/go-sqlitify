package generator

import (
	"strings"
)

const (
	PREFIX_TYPES = "types_"
)

func GenerateFileName(path string) (name string) {
	tokens := strings.Split(path, ".")
	names := tokens[0 : len(tokens)-1]
	name = PREFIX_TYPES + strings.Join(names, "_") + ".go"
	return
}

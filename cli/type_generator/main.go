package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	log "github.com/Sirupsen/logrus"
	flags "github.com/jessevdk/go-flags"
	"github.com/pkg/errors"

	"github.com/t2y/go-sqlitify/lib/generator"
)

type Options struct {
	InputPath  string `long:"inputPath" required:"true" description:"path to input data files"`
	OutputPath string `long:"outputPath" description:"path to output go source code"`

	Verbose bool `long:"verbose" description:"use verbose mode"`
}

var opts Options

var supportFormat map[string]struct{} = map[string]struct{}{
	".json": struct{}{},
}

func readDir(opts *Options) (paths []string, err error) {
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

	infoList, err := ioutil.ReadDir(opts.InputPath)
	if err != nil {
		err = errors.Wrap(err, "failed to read directory")
		return
	}

	for _, info := range infoList {
		if !info.IsDir() {
			name := info.Name()
			if _, ok := supportFormat[filepath.Ext(name)]; ok {
				paths = append(paths, name)
			}
		}
	}

	return
}

func main() {
	fmt.Println("i am type generator")

	if _, err := flags.Parse(&opts); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		}
		os.Exit(1)
	}

	if opts.Verbose {
		log.SetLevel(log.DebugLevel)
	}

	paths, err := readDir(&opts)
	if err != nil || len(paths) == 0 {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to find json files")
	}

	for _, path := range paths {
		var code *generator.TypesCode
		code, err = generator.GenerateTypes(path)
		if err != nil {
			log.WithFields(log.Fields{
				"path": path,
				"err":  err,
			}).Error("Failed to generate type code")
			continue
		}
		name := code.GetFileName()

		var output *os.File
		output, err = os.Create(name)
		if err != nil {
			log.WithFields(log.Fields{
				"name": name,
				"err":  err,
			}).Fatal("Failed to create file")
		}
		output.WriteString(code.GetSource())
		output.Close()
	}
}

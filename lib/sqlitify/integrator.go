package sqlitify

import (
	"os"
	"path/filepath"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/naoina/genmai"
	"github.com/pkg/errors"
)

type DataIntegrator interface {
	Run([]genmai.TableNamer) error
}

type defaultIntegrator struct {
	opts *Options
	db   *ExtDB
}

const (
	TypeSimpleIntegrator = "simple"
	TypeGroupIntegrator  = "group"
)

type SimpleIntegrator struct {
	*defaultIntegrator
}

func (s *SimpleIntegrator) Run(
	tables []genmai.TableNamer,
) (err error) {
	typeName := GetTypeName(s)
	log.WithFields(log.Fields{
		"integrator": typeName,
	}).Debug("start integrating data")

	schemaName := "partof"
	for _, path := range s.opts.OutputPaths {
		log.WithFields(log.Fields{
			"integrator": typeName,
			"path":       path,
		}).Info("merge target")
		if err = s.db.Merge(path, schemaName, tables); err != nil {
			err = errors.Wrap(err, "faild to merge data")
			return
		}
	}

	log.WithFields(log.Fields{
		"integrator": typeName,
	}).Debug("end integrating data")
	return
}

type GroupIntegrator struct {
	*defaultIntegrator
}

func (g *GroupIntegrator) mergeInGroups(
	tables []genmai.TableNamer,
	paths []string,
	numberOfGroups int,
) (mergedPaths []string) {
	pathCh := make(chan []string, g.opts.Concurrent)

	var wg sync.WaitGroup
	for i := 0; i < int(g.opts.Concurrent); i++ {
		wg.Add(1)
		go func(withoutRemove bool) {
			defer wg.Done()
			for {
				group, ok := <-pathCh
				if !ok {
					return
				}

				db, err := NewExtDB(group[0])
				if err != nil {
					log.WithFields(log.Fields{
						"err": err,
					}).Error("Failed to create table")
					return
				}

				if err = db.CreateTablesIfNotExists(tables); err != nil {
					log.WithFields(log.Fields{
						"err": err,
					}).Error("Failed to create table")
					db.Close()
					return
				}

				schemaName := "partof"
				for _, path := range group[1:] {
					if err = db.Merge(path, schemaName, tables); err != nil {
						log.WithFields(log.Fields{
							"err": err,
						}).Error("Failed to merge data")
						db.Close()
						return
					}

					if !withoutRemove {
						if err = os.Remove(path); err != nil {
							log.WithFields(log.Fields{
								"err": err,
							}).Warn("Failed to remove")
						}
					}
				}

				db.Close()
			}
		}(g.opts.WithoutRemoveDB)
	}

	numberOfGroupPaths := len(paths) / numberOfGroups

	var maxMergedPathNum int
	if len(paths)%numberOfGroups == 0 {
		maxMergedPathNum = numberOfGroupPaths
	} else {
		maxMergedPathNum = numberOfGroupPaths + 1
	}

	mergedPaths = make([]string, 0, maxMergedPathNum+(numberOfGroups-1))

	for _, group := range GroupArray(numberOfGroups, paths) {
		if len(group) == numberOfGroups {
			mergedPaths = append(mergedPaths, group[0])
			pathCh <- group
		} else {
			for _, extra := range group {
				mergedPaths = append(mergedPaths, extra)
			}
		}
	}
	close(pathCh)

	wg.Wait()
	return
}

func (g *GroupIntegrator) Run(
	tables []genmai.TableNamer,
) (err error) {
	typeName := GetTypeName(g)
	log.WithFields(log.Fields{
		"integrator": typeName,
	}).Debug("start integrating data")

	paths := g.opts.OutputPaths
	for {
		log.WithFields(log.Fields{
			"number of db files": len(paths),
		}).Info("merge target")

		paths = g.mergeInGroups(tables, paths, 2)
		if len(paths) == 1 {
			break
		}
	}

	resultPath := filepath.Join(g.opts.OutputPath, g.opts.DBName)
	if err = os.Rename(paths[0], resultPath); err != nil {
		err = errors.Wrap(err, "failed to rename a file")
		return
	}

	log.WithFields(log.Fields{
		"integrator": typeName,
	}).Debug("end integrating data")
	return
}

func GroupArray(n int, inputs []string) (outputs [][]string) {
	group := make([]string, 0, n)
	for i, v := range inputs {
		if i%n == 0 {
			if i != 0 {
				outputs = append(outputs, group)
				group = make([]string, 0, n)
			}
		}
		group = append(group, v)
	}

	if len(group) > 0 {
		outputs = append(outputs, group)
	}

	return
}

func NewDataIntegrator(
	opts *Options, db *ExtDB, name string,
) (di DataIntegrator) {
	d := &defaultIntegrator{
		opts: opts,
		db:   db,
	}

	switch name {
	case TypeSimpleIntegrator:
		di = &SimpleIntegrator{d}
	case TypeGroupIntegrator:
		di = &GroupIntegrator{d}
	default:
	}

	return
}

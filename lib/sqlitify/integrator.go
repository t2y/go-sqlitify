package sqlitify

import (
	"os"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/naoina/genmai"
	"github.com/pkg/errors"
)

type DataIntegrator interface {
	Run([]string, []genmai.TableNamer) (string, error)
}

type defaultIntegrator struct {
	opts *Options
}

func (d *defaultIntegrator) mergeThenRemove(
	paths []string, db *ExtDB, tables []genmai.TableNamer,
) (err error) {
	schemaName := "partof"
	for _, path := range paths {
		if err = db.Merge(path, schemaName, tables); err != nil {
			err = errors.Wrap(err, "faild to merge data")
			return
		}

		if !d.opts.WithoutRemoveDB {
			if e := os.Remove(path); e != nil {
				log.WithFields(log.Fields{
					"err": e,
				}).Warn("Failed to remove")
			}
		}
	}

	return
}

const (
	TypeSimpleIntegrator = "simple"
	TypeGroupIntegrator  = "group"
)

type SimpleIntegrator struct {
	*defaultIntegrator
}

func (s *SimpleIntegrator) Run(
	paths []string, tables []genmai.TableNamer,
) (resultPath string, err error) {
	typeName := GetTypeName(s)
	log.WithFields(log.Fields{
		"integrator": typeName,
	}).Debug("start integrating data")

	var db *ExtDB
	resultPath = paths[0]
	if db, err = NewExtDBWithTables(resultPath, tables); err != nil {
		err = errors.Wrap(err, "faild to get db")
		return
	}
	defer db.Close()

	if err = s.mergeThenRemove(paths[1:], db, tables); err != nil {
		err = errors.Wrap(err, "faild to merge and remove")
		return
	}

	log.WithFields(log.Fields{
		"integrator": typeName,
		"resultPath": resultPath,
	}).Debug("end integrating data")
	return
}

type GroupIntegrator struct {
	*defaultIntegrator
}

func (g *GroupIntegrator) getMergedPathSize(
	pathsLength, numberOfGroups int,
) (size int) {
	numberOfGroupPaths := pathsLength / numberOfGroups
	if pathsLength%numberOfGroups != 0 {
		numberOfGroupPaths += 1
	}

	size = numberOfGroupPaths + (numberOfGroups - 1)
	return
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
		go func() {
			defer wg.Done()
			for {
				group, ok := <-pathCh
				if !ok {
					return
				}

				db, err := NewExtDBWithTables(group[0], tables)
				if err != nil {
					log.WithFields(log.Fields{
						"err": err,
					}).Error("failed to get db")
					continue
				}

				if err := g.mergeThenRemove(group[1:], db, tables); err != nil {
					log.WithFields(log.Fields{
						"err": err,
					}).Error("failed to merge and remove")
					db.Close()
					continue
				}

				db.Close()
			}
		}()
	}

	mergedPathSize := g.getMergedPathSize(len(paths), numberOfGroups)
	mergedPaths = make([]string, 0, mergedPathSize)
	for _, group := range GroupSlices(numberOfGroups, paths) {
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
	paths []string, tables []genmai.TableNamer,
) (resultPath string, err error) {
	typeName := GetTypeName(g)
	log.WithFields(log.Fields{
		"integrator": typeName,
	}).Debug("start integrating data")

	for {
		log.WithFields(log.Fields{
			"number of db files": len(paths),
		}).Info("merge target")

		paths = g.mergeInGroups(tables, paths, 2)
		if len(paths) == 1 {
			resultPath = paths[0]
			break
		}
	}

	log.WithFields(log.Fields{
		"integrator": typeName,
		"resultPath": resultPath,
	}).Debug("end integrating data")
	return
}

func GroupSlices(n int, inputs []string) (outputs [][]string) {
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
	opts *Options, typ string,
) (di DataIntegrator) {
	d := &defaultIntegrator{
		opts: opts,
	}

	switch typ {
	case TypeSimpleIntegrator:
		di = &SimpleIntegrator{d}
	case TypeGroupIntegrator:
		di = &GroupIntegrator{d}
	default:
	}

	return
}

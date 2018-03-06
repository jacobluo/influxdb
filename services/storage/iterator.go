package storage

import (
	"errors"

	"github.com/influxdata/influxdb/query"
)

type orderedDedupeIterator struct {
	query.FloatIterator
	last string
	err  error
}

func (itr *orderedDedupeIterator) Next() (*query.FloatPoint, error) {
	if itr.err == nil {
		var p *query.FloatPoint
		for {
			p, itr.err = itr.FloatIterator.Next()
			if p == nil || itr.err != nil {
				break
			}
			key, ok := p.Aux[0].(string)
			if !ok {
				itr.err = errors.New("expected string for series key")
				break
			}

			if itr.last != key {
				itr.last = key
				return p, nil
			}
		}
	}

	return nil, itr.err
}

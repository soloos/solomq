package swald

import (
	"soloos/sdbone/offheap"
)

type SWALD struct {
	options       Options
	offheapDriver *offheap.OffheapDriver
}

func (p *SWALD) Init(options Options) error {
	p.options = options
	return nil
}

func (p *SWALD) Serve() error {
	return nil
}

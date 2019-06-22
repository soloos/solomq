package main

import (
	"soloos/sdbone/offheap"
)

type Env struct {
	options       Options
	offheapDriver *offheap.OffheapDriver
}

func (p *Env) Init(options Options) error {
	p.options = options
	return nil
}

func (p *Env) Serve() error {
	return nil
}

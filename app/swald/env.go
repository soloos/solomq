package main

import (
	"soloos/sdbone/offheap"
)

type Env struct {
	options       Options
	offheapDriver *offheap.OffheapDriver
}

func (p *Env) Init(options Options) {
	p.options = options
}

func (p *Env) Start() {
}

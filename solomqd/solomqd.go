package solomqd

import "soloos/solomq/solomq"

type SolomqD struct {
	options Options
	solomq  solomq.Solomq
}

func (p *SolomqD) Init(options Options) error {
	p.options = options
	return nil
}

func (p *SolomqD) Serve() error {
	return nil
}

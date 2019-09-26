package solomqd

import "soloos/solomq/broker"

type SOLOMQD struct {
	options Options
	broker  broker.Broker
}

func (p *SOLOMQD) Init(options Options) error {
	p.options = options
	return nil
}

func (p *SOLOMQD) Serve() error {
	return nil
}

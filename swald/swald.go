package swald

import "soloos/swal/broker"

type SWALD struct {
	options Options
	broker  broker.Broker
}

func (p *SWALD) Init(options Options) error {
	p.options = options
	return nil
}

func (p *SWALD) Serve() error {
	return nil
}

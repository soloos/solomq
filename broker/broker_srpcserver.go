package broker

import (
	"soloos/common/log"
	"soloos/common/sdfsapitypes"
	"soloos/common/snet"
)

type BrokerSRPCServer struct {
	broker            *Broker
	srpcServerListenAddr string
	srpcServer           snet.SRPCServer
}

func (p *BrokerSRPCServer) Init(broker *Broker, srpcServerListenAddr string) error {
	var err error
	p.broker = broker
	p.srpcServerListenAddr = srpcServerListenAddr
	err = p.srpcServer.Init(sdfsapitypes.DefaultSDFSRPCNetwork, p.srpcServerListenAddr)
	if err != nil {
		return err
	}

	p.srpcServer.RegisterService("/Topic/Prepare", p.ctrTopicPrepare)
	p.srpcServer.RegisterService("/Topic/PWrite", p.ctrTopicPWrite)

	return nil
}

func (p *BrokerSRPCServer) Serve() error {
	var err error
	log.Info("broker srpcserver serve at:", p.srpcServerListenAddr)
	err = p.srpcServer.Serve()
	return err
}

func (p *BrokerSRPCServer) Close() error {
	var err error
	log.Info("broker srpcserver close at:", p.srpcServerListenAddr)
	err = p.srpcServer.Close()
	return err
}

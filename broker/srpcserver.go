package broker

import (
	"soloos/common/iron"
	"soloos/common/log"
	"soloos/common/sdfsapitypes"
	"soloos/common/snet"
)

type SRPCServer struct {
	broker               *Broker
	srpcServerListenAddr string
	srpcServer           snet.SRPCServer
}

var _ = iron.IServer(&SRPCServer{})

func (p *SRPCServer) Init(broker *Broker, srpcServerListenAddr string) error {
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

func (p *SRPCServer) ServerName() string {
	return "SoloOS.SWAL.Broker.SRPCServer"
}

func (p *SRPCServer) Serve() error {
	var err error
	log.Info("broker srpcserver serve at:", p.srpcServerListenAddr)
	err = p.srpcServer.Serve()
	return err
}

func (p *SRPCServer) Close() error {
	var err error
	log.Info("broker srpcserver close at:", p.srpcServerListenAddr)
	err = p.srpcServer.Close()
	return err
}

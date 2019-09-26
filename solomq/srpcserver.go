package solomq

import (
	"soloos/common/iron"
	"soloos/common/log"
	"soloos/common/solofsapitypes"
	"soloos/common/snet"
)

type SRPCServer struct {
	solomq               *Solomq
	srpcServerListenAddr string
	srpcServer           snet.SRPCServer
}

var _ = iron.IServer(&SRPCServer{})

func (p *SRPCServer) Init(solomq *Solomq, srpcServerListenAddr string) error {
	var err error
	p.solomq = solomq
	p.srpcServerListenAddr = srpcServerListenAddr
	err = p.srpcServer.Init(solofsapitypes.DefaultSolofsRPCNetwork, p.srpcServerListenAddr)
	if err != nil {
		return err
	}

	p.srpcServer.RegisterService("/Topic/Prepare", p.ctrTopicPrepare)
	p.srpcServer.RegisterService("/Topic/PWrite", p.ctrTopicPWrite)

	return nil
}

func (p *SRPCServer) ServerName() string {
	return "Soloos.Solomq.Solomq.SRPCServer"
}

func (p *SRPCServer) Serve() error {
	var err error
	log.Info("solomq srpcserver serve at:", p.srpcServerListenAddr)
	err = p.srpcServer.Serve()
	return err
}

func (p *SRPCServer) Close() error {
	var err error
	log.Info("solomq srpcserver close at:", p.srpcServerListenAddr)
	err = p.srpcServer.Close()
	return err
}

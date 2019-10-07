package solomq

import (
	"soloos/common/iron"
	"soloos/common/log"
	"soloos/common/snet"
	"soloos/common/solofstypes"
)

type SrpcServer struct {
	solomq               *Solomq
	srpcServerListenAddr string
	srpcServer           snet.SrpcServer
}

var _ = iron.IServer(&SrpcServer{})

func (p *SrpcServer) Init(solomq *Solomq, srpcServerListenAddr string) error {
	var err error
	p.solomq = solomq
	p.srpcServerListenAddr = srpcServerListenAddr
	err = p.srpcServer.Init(solofstypes.DefaultSolofsRPCNetwork, p.srpcServerListenAddr)
	if err != nil {
		return err
	}

	p.srpcServer.RegisterService("/Topic/Prepare", p.ctrTopicPrepare)
	p.srpcServer.RegisterService("/Topic/PWrite", p.ctrTopicPWrite)

	return nil
}

func (p *SrpcServer) ServerName() string {
	return "Soloos.Solomq.Solomq.SrpcServer"
}

func (p *SrpcServer) Serve() error {
	var err error
	log.Info("solomq srpcserver serve at:", p.srpcServerListenAddr)
	err = p.srpcServer.Serve()
	return err
}

func (p *SrpcServer) Close() error {
	var err error
	log.Info("solomq srpcserver close at:", p.srpcServerListenAddr)
	err = p.srpcServer.Close()
	return err
}

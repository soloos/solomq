package agent

import (
	"soloos/common/log"
	"soloos/common/sdfsapitypes"
	"soloos/common/snet"
)

type SWALAgentSRPCServer struct {
	swalAgent            *SWALAgent
	srpcServerListenAddr string
	srpcServer           snet.SRPCServer
}

func (p *SWALAgentSRPCServer) Init(swalAgent *SWALAgent, srpcServerListenAddr string) error {
	var err error
	p.swalAgent = swalAgent
	p.srpcServerListenAddr = srpcServerListenAddr
	err = p.srpcServer.Init(sdfsapitypes.DefaultSDFSRPCNetwork, p.srpcServerListenAddr)
	if err != nil {
		return err
	}

	p.srpcServer.RegisterService("/Topic/Prepare", p.ctrTopicPrepare)
	p.srpcServer.RegisterService("/Topic/PWrite", p.ctrTopicPWrite)

	return nil
}

func (p *SWALAgentSRPCServer) Serve() error {
	var err error
	log.Info("swalagent srpcserver serve at:", p.srpcServerListenAddr)
	err = p.srpcServer.Serve()
	return err
}

func (p *SWALAgentSRPCServer) Close() error {
	var err error
	log.Info("swalagent srpcserver close at:", p.srpcServerListenAddr)
	err = p.srpcServer.Close()
	return err
}

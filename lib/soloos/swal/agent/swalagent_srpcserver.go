package agent

import (
	"soloos/common/sdfsapitypes"
	"soloos/common/snet/srpc"
)

type SWALAgentSRPCServer struct {
	swalAgent            *SWALAgent
	srpcServerListenAddr string
	srpcServer           srpc.Server
}

func (p *SWALAgentSRPCServer) Init(swalAgent *SWALAgent, srpcServerListenAddr string) error {
	var err error
	p.swalAgent = swalAgent
	p.srpcServerListenAddr = srpcServerListenAddr
	err = p.srpcServer.Init(sdfsapitypes.DefaultSDFSRPCNetwork, p.srpcServerListenAddr)
	if err != nil {
		return err
	}

	p.srpcServer.RegisterService("/Topic/FsINode/Prepare", p.ctrTopicNetINodePrepare)
	p.srpcServer.RegisterService("/Topic/NetINode/PWrite", p.ctrTopicNetINodePWrite)

	return nil
}

func (p *SWALAgentSRPCServer) Serve() error {
	var err error
	err = p.srpcServer.Serve()
	return err
}

func (p *SWALAgentSRPCServer) Close() error {
	var err error
	err = p.srpcServer.Close()
	return err
}

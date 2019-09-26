package solomq

import (
	"fmt"
	"soloos/common/fsapi"
	"soloos/common/iron"
	"soloos/common/snet"
	"soloos/common/snettypes"
	"soloos/common/solodbapi"
	"soloos/common/solofsapi"
	"soloos/common/solomqapi"
	"soloos/common/solomqapitypes"
	"soloos/common/soloosbase"
)

type Solomq struct {
	*soloosbase.SoloosEnv
	srpcPeer snettypes.Peer
	webPeer  snettypes.Peer
	dbConn   solodbapi.Connection

	TopicDriver
	solomqClient solomqapi.SolomqClient

	solofsClient solofsapi.Client
	posixFS      fsapi.PosixFS

	localFsSNetPeer snettypes.Peer

	heartBeatServerOptionsArr []snettypes.HeartBeatServerOptions
	srpcServer                SRPCServer
	serverDriver              iron.ServerDriver
}

func (p *Solomq) initLocalFs() error {
	var err error
	p.localFsSNetPeer.ID = snet.MakeSysPeerID(fmt.Sprintf("Solomq_LOCAL_FS"))
	p.localFsSNetPeer.SetAddress("LocalFs")
	p.localFsSNetPeer.ServiceProtocol = snettypes.ProtocolLocalFS
	err = p.SNetDriver.RegisterPeer(p.localFsSNetPeer)
	if err != nil {
		return err
	}
	return nil
}

func (p *Solomq) initSNetPeer(peerID snettypes.PeerID, srpcListenAddr string) error {
	var err error

	p.srpcPeer.ID = peerID
	p.srpcPeer.SetAddress(srpcListenAddr)
	p.srpcPeer.ServiceProtocol = solomqapitypes.DefaultSolomqRPCProtocol
	err = p.SoloosEnv.SNetDriver.RegisterPeer(p.srpcPeer)
	if err != nil {
		return err
	}

	return nil
}

func (p *Solomq) Init(soloosEnv *soloosbase.SoloosEnv,
	srpcPeerID snettypes.PeerID, srpcListenAddr string,
	dbDriver string, dsn string,
	defaultNetBlockCap int, defaultMemBlockCap int,
) error {
	var err error

	p.SoloosEnv = soloosEnv

	err = p.initSNetPeer(srpcPeerID, srpcListenAddr)
	if err != nil {
		return err
	}

	err = p.dbConn.Init(dbDriver, dsn)
	if err != nil {
		return err
	}

	err = p.installSchema(dbDriver)
	if err != nil {
		return err
	}

	err = p.solomqClient.Init(p.SoloosEnv)
	if err != nil {
		return err
	}

	err = p.TopicDriver.Init(p, defaultNetBlockCap, defaultMemBlockCap)
	if err != nil {
		return err
	}

	err = p.srpcServer.Init(p, srpcListenAddr)
	if err != nil {
		return err
	}

	err = p.serverDriver.Init(&p.srpcServer)
	if err != nil {
		return err
	}

	err = p.initLocalFs()
	if err != nil {
		return err
	}

	err = p.RegisterInDB()
	if err != nil {
		return err
	}

	return nil
}

func (p *Solomq) Serve() error {
	var err error

	err = p.StartHeartBeat()
	if err != nil {
		return err
	}

	err = p.serverDriver.Serve()
	if err != nil {
		return err
	}

	return nil
}

func (p *Solomq) Close() error {
	var err error

	err = p.serverDriver.Close()
	if err != nil {
		return err
	}

	return nil
}

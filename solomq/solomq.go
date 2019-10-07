package solomq

import (
	"fmt"
	"soloos/common/iron"
	"soloos/common/snet"
	"soloos/common/solodbapi"
	"soloos/common/solomqapi"
	"soloos/common/solomqtypes"
	"soloos/common/soloosbase"
	"soloos/solofs/solofssdk"
	"time"
)

type Solomq struct {
	*soloosbase.SoloosEnv
	srpcPeer snet.Peer
	webPeer  snet.Peer
	dbConn   solodbapi.Connection

	TopicDriver

	solomqClient solomqapi.SolomqClient
	solofsClient *solofssdk.Client

	localFsSNetPeer snet.Peer

	heartBeatServerOptionsArr []snet.HeartBeatServerOptions
	srpcServer                SrpcServer
	serverDriver              iron.ServerDriver

	normalCallRetryTimes        int
	waitAliveEveryRetryWaitTime time.Duration
}

func (p *Solomq) initLocalFs() error {
	var err error
	p.localFsSNetPeer.ID = snet.MakeSysPeerID(fmt.Sprintf("Solomq_LOCAL_FS"))
	p.localFsSNetPeer.SetAddress("LocalFs")
	p.localFsSNetPeer.ServiceProtocol = snet.ProtocolLocalFs
	err = p.SNetDriver.RegisterPeer(p.localFsSNetPeer)
	if err != nil {
		return err
	}
	return nil
}

func (p *Solomq) initSNetPeer(peerID snet.PeerID, srpcListenAddr string) error {
	var err error

	p.srpcPeer.ID = peerID
	p.srpcPeer.SetAddress(srpcListenAddr)
	p.srpcPeer.ServiceProtocol = solomqtypes.DefaultSolomqRPCProtocol
	err = p.SoloosEnv.SNetDriver.RegisterPeer(p.srpcPeer)
	if err != nil {
		return err
	}

	return nil
}

func (p *Solomq) Init(soloosEnv *soloosbase.SoloosEnv,
	srpcPeerID snet.PeerID, srpcListenAddr string,
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

	p.normalCallRetryTimes = 3
	p.waitAliveEveryRetryWaitTime = time.Second * 3

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

package broker

import (
	"fmt"
	"soloos/common/fsapi"
	"soloos/common/iron"
	"soloos/common/solodbapi"
	"soloos/common/solofsapi"
	"soloos/common/snet"
	"soloos/common/snettypes"
	"soloos/common/soloosbase"
	"soloos/common/solomqapi"
	"soloos/common/solomqapitypes"
)

type Broker struct {
	*soloosbase.SoloOSEnv
	srpcPeer snettypes.Peer
	webPeer  snettypes.Peer
	dbConn   solodbapi.Connection

	TopicDriver
	brokerClient solomqapi.BrokerClient

	solofsClient solofsapi.Client
	posixFS    fsapi.PosixFS

	localFsSNetPeer snettypes.Peer

	heartBeatServerOptionsArr []snettypes.HeartBeatServerOptions
	srpcServer                SRPCServer
	serverDriver              iron.ServerDriver
}

func (p *Broker) initLocalFs() error {
	var err error
	p.localFsSNetPeer.ID = snet.MakeSysPeerID(fmt.Sprintf("Broker_LOCAL_FS"))
	p.localFsSNetPeer.SetAddress("LocalFs")
	p.localFsSNetPeer.ServiceProtocol = snettypes.ProtocolLocalFS
	err = p.SNetDriver.RegisterPeer(p.localFsSNetPeer)
	if err != nil {
		return err
	}
	return nil
}

func (p *Broker) initSNetPeer(peerID snettypes.PeerID, srpcListenAddr string) error {
	var err error

	p.srpcPeer.ID = peerID
	p.srpcPeer.SetAddress(srpcListenAddr)
	p.srpcPeer.ServiceProtocol = solomqapitypes.DefaultSOLOMQRPCProtocol
	err = p.SoloOSEnv.SNetDriver.RegisterPeer(p.srpcPeer)
	if err != nil {
		return err
	}

	return nil
}

func (p *Broker) Init(soloOSEnv *soloosbase.SoloOSEnv,
	srpcPeerID snettypes.PeerID, srpcListenAddr string,
	dbDriver string, dsn string,
	defaultNetBlockCap int, defaultMemBlockCap int,
) error {
	var err error

	p.SoloOSEnv = soloOSEnv

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

	err = p.brokerClient.Init(p.SoloOSEnv)
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

func (p *Broker) Serve() error {
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

func (p *Broker) Close() error {
	var err error

	err = p.serverDriver.Close()
	if err != nil {
		return err
	}

	return nil
}

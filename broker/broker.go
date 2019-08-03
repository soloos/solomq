package broker

import (
	"fmt"
	"soloos/common/fsapi"
	"soloos/common/sdbapi"
	"soloos/common/sdfsapi"
	"soloos/common/snet"
	"soloos/common/snettypes"
	"soloos/common/soloosbase"
	"soloos/common/swalapi"
	"soloos/common/swalapitypes"
)

type Broker struct {
	*soloosbase.SoloOSEnv
	srpcPeer snettypes.Peer
	webPeer  snettypes.Peer
	dbConn   sdbapi.Connection

	TopicDriver
	brokerClient swalapi.BrokerClient

	sdfsClient sdfsapi.Client
	posixFS    fsapi.PosixFS

	localFsSNetPeer snettypes.Peer

	heartBeatServerOptionsArr []swalapitypes.HeartBeatServerOptions
	srpcServer                BrokerSRPCServer
	webServer                 BrokerSRPCServer
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

func (p *Broker) initSNetPeer(peerID snettypes.PeerID, srpcServeAddr string) error {
	var err error

	p.srpcPeer.ID = peerID
	p.srpcPeer.SetAddress(srpcServeAddr)
	p.srpcPeer.ServiceProtocol = swalapitypes.DefaultSWALRPCProtocol
	err = p.SoloOSEnv.SNetDriver.RegisterPeer(p.srpcPeer)
	if err != nil {
		return err
	}

	return nil
}

func (p *Broker) Init(soloOSEnv *soloosbase.SoloOSEnv,
	srpcPeerID snettypes.PeerID, srpcServeAddr string,
	dbDriver string, dsn string,
	defaultNetBlockCap int, defaultMemBlockCap int,
) error {
	var err error

	p.SoloOSEnv = soloOSEnv

	err = p.initSNetPeer(srpcPeerID, srpcServeAddr)
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

	err = p.srpcServer.Init(p, srpcServeAddr)
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
	err = p.srpcServer.Serve()
	return err
}

func (p *Broker) Close() error {
	var err error
	err = p.srpcServer.Close()
	if err != nil {
		return err
	}

	return nil
}

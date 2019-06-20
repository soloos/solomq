package agent

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

type SWALAgent struct {
	*soloosbase.SoloOSEnv
	peer   snettypes.Peer
	dbConn sdbapi.Connection

	TopicDriver
	swalAgentClient swalapi.SWALAgentClient

	sdfsClient sdfsapi.Client
	posixFS    fsapi.PosixFS

	localFsSNetPeer snettypes.Peer

	srpcServer SWALAgentSRPCServer
}

func (p *SWALAgent) initLocalFs() error {
	var err error
	p.localFsSNetPeer.ID = snet.MakeSysPeerID(fmt.Sprintf("SWALAgent_LOCAL_FS"))
	p.localFsSNetPeer.SetAddress("LocalFs")
	p.localFsSNetPeer.ServiceProtocol = snettypes.ProtocolDisk
	err = p.SNetDriver.RegisterPeer(p.localFsSNetPeer)
	if err != nil {
		return err
	}
	return nil
}

func (p *SWALAgent) initSNetPeer(peerID snettypes.PeerID, serveAddr string) error {
	var err error
	p.peer.ID = peerID
	p.peer.SetAddress(serveAddr)
	p.peer.ServiceProtocol = swalapitypes.DefaultSWALRPCProtocol

	err = p.SoloOSEnv.SNetDriver.RegisterPeer(p.peer)
	if err != nil {
		return err
	}

	return nil
}

func (p *SWALAgent) Init(soloOSEnv *soloosbase.SoloOSEnv,
	peerID snettypes.PeerID, serveAddr string,
	dbDriver string, dsn string,
	defaultNetBlockCap int, defaultMemBlockCap int,
) error {
	var err error

	p.SoloOSEnv = soloOSEnv

	err = p.initSNetPeer(peerID, serveAddr)
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

	err = p.swalAgentClient.Init(p.SoloOSEnv)
	if err != nil {
		return err
	}

	err = p.TopicDriver.Init(p, defaultNetBlockCap, defaultMemBlockCap)
	if err != nil {
		return err
	}

	err = p.srpcServer.Init(p, serveAddr)
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

func (p *SWALAgent) GetPeerID() snettypes.PeerID {
	return p.peer.ID
}

func (p *SWALAgent) Serve() error {
	var err error
	err = p.srpcServer.Serve()
	return err
}

func (p *SWALAgent) Close() error {
	var err error
	err = p.srpcServer.Close()
	if err != nil {
		return err
	}

	return nil
}

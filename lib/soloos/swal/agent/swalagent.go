package agent

import (
	"soloos/common/fsapi"
	"soloos/common/log"
	"soloos/common/sdbapi"
	"soloos/common/sdfsapi"
	"soloos/common/snettypes"
	"soloos/common/soloosbase"
	"soloos/common/swalapi"
	"soloos/common/swalapitypes"
	"soloos/common/util"
)

type SWALAgent struct {
	*soloosbase.SoloOSEnv
	peer       snettypes.Peer
	dbConn     sdbapi.Connection
	srpcServer SWALAgentSRPCServer
	uploader   swalAgentUploader

	sdfsClient sdfsapi.Client
	posixFS    fsapi.PosixFS

	TopicDriver

	SWALAgentClient swalapi.SWALAgentClient
}

func (p *SWALAgent) Init(soloOSEnv *soloosbase.SoloOSEnv,
	peerID snettypes.PeerID, serveAddr string,
	dbDriver string, dsn string,
	defaultNetBlockCap int, defaultMemBlockCap int,
) error {
	var err error

	p.SoloOSEnv = soloOSEnv
	p.peer.ID = peerID
	p.peer.SetAddress(serveAddr)
	p.peer.ServiceProtocol = swalapitypes.DefaultSWALRPCProtocol

	err = p.dbConn.Init(dbDriver, dsn)
	if err != nil {
		return err
	}

	err = p.installSchema(dbDriver)
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

	err = p.RegisterInDB()
	if err != nil {
		return err
	}

	err = p.SoloOSEnv.SNetDriver.RegisterPeer(p.peer)
	log.Error("fuck register", p.peer.PeerIDStr(), p.peer.AddressStr())
	if err != nil {
		return err
	}

	err = p.uploader.Init(p)
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
	go func() {
		util.AssertErrIsNil(p.uploader.Serve())
	}()
	return err
}

func (p *SWALAgent) Close() error {
	var err error
	err = p.srpcServer.Close()
	if err != nil {
		return err
	}

	err = p.uploader.Close()
	if err != nil {
		return err
	}

	return nil
}

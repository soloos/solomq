package solomqsdk

import (
	"soloos/common/snettypes"
	"soloos/common/solomqapi"
	"soloos/common/solomqapitypes"
	"soloos/common/soloosbase"
	"soloos/solomq/solomq"
)

type ClientDriver struct {
	*soloosbase.SoloosEnv
	solomq solomq.Solomq
}

var _ = solomqapi.ClientDriver(&ClientDriver{})

func (p *ClientDriver) Init(soloosEnv *soloosbase.SoloosEnv,
	soloBoatWebPeerID string,
	solomqSRPCPeerIDStr string, solomqSRPCServeAddr string,
	dbDriver string, dsn string,
	defaultNetBlockCap int, defaultMemBlockCap int,
) error {
	var err error

	p.SoloosEnv = soloosEnv

	var solomqSRPCPeerID snettypes.PeerID
	copy(solomqSRPCPeerID[:], []byte(solomqSRPCPeerIDStr))
	err = p.solomq.Init(p.SoloosEnv,
		solomqSRPCPeerID, solomqSRPCServeAddr,
		dbDriver, dsn,
		defaultNetBlockCap, defaultMemBlockCap,
	)
	if err != nil {
		return err
	}

	var heartBeatServer snettypes.HeartBeatServerOptions
	heartBeatServer.PeerID = snettypes.StrToPeerID(soloBoatWebPeerID)
	heartBeatServer.DurationMS = DefaultHeartBeatDurationMS
	err = p.solomq.SetHeartBeatServers([]snettypes.HeartBeatServerOptions{heartBeatServer})
	if err != nil {
		return err
	}

	return nil
}

func (p *ClientDriver) InitClient(itClient solomqapi.Client,
	topicIDStr string, solomqMembers []solomqapitypes.SolomqMember,
) error {

	var err error
	client := itClient.(*Client)
	err = client.Init(p, topicIDStr, solomqMembers)
	if err != nil {
		return err
	}

	return nil
}

func (p *ClientDriver) Serve() error {
	return p.solomq.Serve()
}

func (p *ClientDriver) Close() error {
	return p.solomq.Close()
}

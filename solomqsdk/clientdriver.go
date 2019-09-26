package solomqsdk

import (
	"soloos/common/snettypes"
	"soloos/common/soloosbase"
	"soloos/common/solomqapi"
	"soloos/common/solomqapitypes"
	"soloos/solomq/broker"
)

type ClientDriver struct {
	*soloosbase.SoloOSEnv
	broker broker.Broker
}

var _ = solomqapi.ClientDriver(&ClientDriver{})

func (p *ClientDriver) Init(soloOSEnv *soloosbase.SoloOSEnv,
	soloBoatWebPeerID string,
	brokerSRPCPeerIDStr string, brokerSRPCServeAddr string,
	dbDriver string, dsn string,
	defaultNetBlockCap int, defaultMemBlockCap int,
) error {
	var err error

	p.SoloOSEnv = soloOSEnv

	var brokerSRPCPeerID snettypes.PeerID
	copy(brokerSRPCPeerID[:], []byte(brokerSRPCPeerIDStr))
	err = p.broker.Init(p.SoloOSEnv,
		brokerSRPCPeerID, brokerSRPCServeAddr,
		dbDriver, dsn,
		defaultNetBlockCap, defaultMemBlockCap,
	)
	if err != nil {
		return err
	}

	var heartBeatServer snettypes.HeartBeatServerOptions
	heartBeatServer.PeerID = snettypes.StrToPeerID(soloBoatWebPeerID)
	heartBeatServer.DurationMS = DefaultHeartBeatDurationMS
	err = p.broker.SetHeartBeatServers([]snettypes.HeartBeatServerOptions{heartBeatServer})
	if err != nil {
		return err
	}

	return nil
}

func (p *ClientDriver) InitClient(itClient solomqapi.Client,
	topicIDStr string, solomqMembers []solomqapitypes.SOLOMQMember,
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
	return p.broker.Serve()
}

func (p *ClientDriver) Close() error {
	return p.broker.Close()
}

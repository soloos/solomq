package libswal

import (
	"soloos/common/snettypes"
	"soloos/common/soloosbase"
	"soloos/common/swalapi"
	"soloos/common/swalapitypes"
	"soloos/swal/agent"
)

type ClientDriver struct {
	*soloosbase.SoloOSEnv
	SWALAgent agent.SWALAgent
}

var _ = swalapi.ClientDriver(&ClientDriver{})

func (p *ClientDriver) Init(soloOSEnv *soloosbase.SoloOSEnv,
	swalAgentPeerIDStr string, swalAgentServeAddr string,
	dbDriver string, dsn string,
	defaultNetBlockCap int, defaultMemBlockCap int,
) error {
	var err error

	p.SoloOSEnv = soloOSEnv

	var swalAgentPeerID snettypes.PeerID
	copy(swalAgentPeerID[:], []byte(swalAgentPeerIDStr))
	err = p.SWALAgent.Init(p.SoloOSEnv,
		swalAgentPeerID, swalAgentServeAddr,
		dbDriver, dsn,
		defaultNetBlockCap, defaultMemBlockCap,
	)
	if err != nil {
		return err
	}

	return nil
}

func (p *ClientDriver) InitClient(itClient swalapi.Client,
	topicIDStr string, swalMembers []swalapitypes.SWALMember,
) error {

	var err error
	client := itClient.(*Client)
	err = client.Init(p, topicIDStr, swalMembers)
	if err != nil {
		return err
	}

	return nil
}

func (p *ClientDriver) Serve() error {
	return p.SWALAgent.Serve()
}

func (p *ClientDriver) Close() error {
	return p.SWALAgent.Close()
}

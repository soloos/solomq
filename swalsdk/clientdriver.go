package swalsdk

import (
	"soloos/common/snettypes"
	"soloos/common/soloosbase"
	"soloos/common/swalapi"
	"soloos/common/swalapitypes"
	"soloos/swal/broker"
)

type ClientDriver struct {
	*soloosbase.SoloOSEnv
	broker broker.Broker
}

var _ = swalapi.ClientDriver(&ClientDriver{})

func (p *ClientDriver) Init(soloOSEnv *soloosbase.SoloOSEnv,
	brokerPeerIDStr string, brokerServeAddr string,
	dbDriver string, dsn string,
	defaultNetBlockCap int, defaultMemBlockCap int,
) error {
	var err error

	p.SoloOSEnv = soloOSEnv

	var brokerPeerID snettypes.PeerID
	copy(brokerPeerID[:], []byte(brokerPeerIDStr))
	err = p.broker.Init(p.SoloOSEnv,
		brokerPeerID, brokerServeAddr,
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
	return p.broker.Serve()
}

func (p *ClientDriver) Close() error {
	return p.broker.Close()
}

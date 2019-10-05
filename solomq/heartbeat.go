package solomq

import (
	"soloos/common/iron"
	"soloos/common/log"
	"soloos/common/snettypes"
	"soloos/common/solomqapitypes"
	"time"
)

func (p *Solomq) SetHeartBeatServers(heartBeatServerOptionsArr []snettypes.HeartBeatServerOptions) error {
	p.heartBeatServerOptionsArr = heartBeatServerOptionsArr
	return nil
}

func (p *Solomq) doHeartBeat(options snettypes.HeartBeatServerOptions) {
	var (
		heartBeat solomqapitypes.SolomqHeartBeat
		webret    iron.ResponseJSON
		peer      snettypes.Peer
		urlPath   string
		err       error
	)

	heartBeat.SrpcPeerID = p.srpcPeer.PeerID().Str()
	heartBeat.WebPeerID = p.webPeer.PeerID().Str()

	for {
		peer, err = p.SoloosEnv.SNetDriver.GetPeer(options.PeerID)
		urlPath = peer.AddressStr() + "/Api/Solomq/Solomq/HeartBeat"
		if err != nil {
			log.Error("Solomq HeartBeat post json error, urlPath:", urlPath, ", err:", err)
			goto HEARTBEAT_DONE
		}

		err = iron.PostJSON(urlPath, heartBeat, &webret)
		if err != nil {
			log.Error("Solomq HeartBeat post json(decode) error, urlPath:", urlPath, ", err:", err)
			goto HEARTBEAT_DONE
		}
		log.Info("Solomq heartbeat message:", webret)

	HEARTBEAT_DONE:
		time.Sleep(time.Duration(options.DurationMS) * time.Millisecond)
	}
}

func (p *Solomq) StartHeartBeat() error {
	for _, options := range p.heartBeatServerOptionsArr {
		go p.doHeartBeat(options)
	}
	return nil
}

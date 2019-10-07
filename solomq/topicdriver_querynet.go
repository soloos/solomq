package solomq

import (
	"soloos/common/log"
	"soloos/common/snet"
	"soloos/common/solofsapitypes"
	"soloos/common/solomqapitypes"
	"soloos/common/solomqprotocol"
	"time"
)

func (p *TopicDriver) PrepareTopicMetaDataToNet(peerID snet.PeerID,
	uTopic solomqapitypes.TopicUintptr,
	fsINodeID solofsapitypes.FsINodeID,
) error {
	var (
		req    solomqprotocol.TopicPrepareReq
		pTopic = uTopic.Ptr()
		err    error
	)

	req.TopicID = pTopic.ID
	req.FsINodeID = fsINodeID

	for i := 0; i < p.solomq.normalCallRetryTimes; i++ {
		err = p.solomq.solomqClient.SimpleCall(peerID,
			"/Topic/Prepare", nil, req)
		if err == nil {
			break
		}
		log.Info("Topic/Prepare peerID:", peerID.Str(),
			", topicid:", pTopic.ID,
			", retryTimes:", i,
			", err", err)
		time.Sleep(p.solomq.waitAliveEveryRetryWaitTime)
	}
	if err != nil {
		return err
	}

	return nil
}

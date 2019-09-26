package solomq

import (
	"soloos/common/solofsapitypes"
)

func (p *TopicDriver) UploadMemBlockWithSolomq(uJob solofsapitypes.UploadMemBlockJobUintptr,
	uploadPeerIndex int) error {
	var topicID = NetINodeBlockPlacementPolicyGetTopicID(
		&uJob.Ptr().UNetINode.Ptr().MemBlockPlacementPolicy)
	var uTopic, err = p.GetTopicByID(topicID)
	defer p.ReleaseTopic(uTopic)
	if err != nil {
		return err
	}

	return p.solomq.solomqClient.UploadMemBlockWithSolomq(uTopic, uJob, uploadPeerIndex)
}

func (p *TopicDriver) UploadMemBlockWithDisk(uJob solofsapitypes.UploadMemBlockJobUintptr,
	uploadPeerIndex int,
) error {
	return nil
}

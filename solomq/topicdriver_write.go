package solomq

import (
	"soloos/common/solofstypes"
)

func (p *TopicDriver) UploadMemBlockWithSolomq(uJob solofstypes.UploadMemBlockJobUintptr,
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

func (p *TopicDriver) UploadMemBlockWithDisk(uJob solofstypes.UploadMemBlockJobUintptr,
	uploadPeerIndex int,
) error {
	return nil
}

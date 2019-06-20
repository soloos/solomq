package agent

import (
	"soloos/common/sdfsapitypes"
)

func (p *TopicDriver) UploadMemBlockWithSWAL(uJob sdfsapitypes.UploadMemBlockJobUintptr,
	uploadPeerIndex int) error {
	var topicID = NetINodeBlockPlacementPolicyGetTopicID(
		&uJob.Ptr().UNetINode.Ptr().MemBlockPlacementPolicy)
	var uTopic, err = p.GetTopicByID(topicID)
	defer p.ReleaseTopic(uTopic)
	if err != nil {
		return err
	}

	return p.swalAgent.swalAgentClient.UploadMemBlockWithSWAL(uTopic, uJob, uploadPeerIndex)
}

func (p *TopicDriver) UploadMemBlockWithDisk(uJob sdfsapitypes.UploadMemBlockJobUintptr,
	uploadPeerIndex int,
) error {
	return nil
}

package solomq

import (
	"soloos/common/snet"
	"soloos/common/solofstypes"
	"soloos/common/solomqtypes"
	"unsafe"
)

func NetINodeBlockPlacementPolicySetTopicID(
	pPolicy *solofstypes.MemBlockPlacementPolicy,
	topicID solomqtypes.TopicID,
) {
	*(*solomqtypes.TopicID)(unsafe.Pointer(&(pPolicy[solofstypes.MemBlockPlacementPolicyBodyOff]))) =
		solomqtypes.TopicID(topicID)
}

func NetINodeBlockPlacementPolicyGetTopicID(
	pPolicy *solofstypes.MemBlockPlacementPolicy,
) solomqtypes.TopicID {
	return (*(*solomqtypes.TopicID)(unsafe.Pointer(&(pPolicy[solofstypes.MemBlockPlacementPolicyBodyOff]))))
}

func (p *TopicDriver) prepareNetBlockMetaDataWithRoleLeader(uTopic solomqtypes.TopicUintptr,
	uNetBlock solofstypes.NetBlockUintptr,
	uNetINode solofstypes.NetINodeUintptr, netblockIndex int32) error {
	var (
		err               error
		pTopic                       = uTopic.Ptr()
		pNetBlock                    = uNetBlock.Ptr()
		queryNetJobsCount            = pTopic.Meta.SolomqMemberGroup.Len
		queryNetRetArr    chan error = make(chan error, queryNetJobsCount)
	)

	for _, solomqMember := range pTopic.Meta.SolomqMemberGroup.Slice() {
		go func(peerID snet.PeerID, uTopic solomqtypes.TopicUintptr, queryNetRetArr chan error) {
			queryNetRetArr <- p.solomq.solomqClient.PrepareTopicNetBlockMetaData(peerID,
				uTopic, uNetBlock, uNetINode, netblockIndex)
		}(solomqMember.PeerID, uTopic, queryNetRetArr)
	}

	{
		var tmpErr error
		for i := 0; i < queryNetJobsCount; i++ {
			tmpErr = <-queryNetRetArr
			if err != nil {
				err = tmpErr
			}
		}
	}
	if err != nil {
		return err
	}

	pNetBlock.SyncDataBackends.Reset()
	for i := 0; i < pNetBlock.StorDataBackends.Len; i++ {
		if pTopic.Meta.SolomqMemberGroup.Arr[i].PeerID == p.solomq.srpcPeer.ID {
			pNetBlock.SyncDataBackends.Append(pNetBlock.StorDataBackends.Arr[i], 0)
		} else {
			pNetBlock.SyncDataBackends.Append(pTopic.Meta.SolomqMemberGroup.Arr[i].PeerID, 1)
			pNetBlock.SyncDataBackends.Append(pNetBlock.StorDataBackends.Arr[i], 0)
		}
	}

	return nil
}

func (p *TopicDriver) prepareNetBlockMetaDataWithRoleFollower(uTopic solomqtypes.TopicUintptr,
	uNetBlock solofstypes.NetBlockUintptr,
	uNetINode solofstypes.NetINodeUintptr, netblockIndex int32) error {
	panic("fuck shit")
	return nil
}

func (p *TopicDriver) PrepareNetBlockMetaData(topicID solomqtypes.TopicID,
	uNetBlock solofstypes.NetBlockUintptr,
	uNetINode solofstypes.NetINodeUintptr, netblockIndex int32) error {
	var uTopic, err = p.GetTopicByID(topicID)
	defer p.ReleaseTopic(uTopic)
	if err != nil {
		return err
	}

	NetINodeBlockPlacementPolicySetTopicID(&uNetINode.Ptr().MemBlockPlacementPolicy, topicID)

	switch p.computeTopicRole(uTopic) {
	case solomqtypes.SolomqMemberRoleLeader:
		err = p.prepareNetBlockMetaDataWithRoleLeader(uTopic, uNetBlock, uNetINode, netblockIndex)
	case solomqtypes.SolomqMemberRoleFollower:
		err = p.prepareNetBlockMetaDataWithRoleFollower(uTopic, uNetBlock, uNetINode, netblockIndex)
	}

	return nil
}

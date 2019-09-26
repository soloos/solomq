package broker

import (
	"soloos/common/solofsapitypes"
	"soloos/common/snettypes"
	"soloos/common/solomqapitypes"
	"unsafe"
)

func NetINodeBlockPlacementPolicySetTopicID(
	pPolicy *solofsapitypes.MemBlockPlacementPolicy,
	topicID solomqapitypes.TopicID,
) {
	*(*solomqapitypes.TopicID)(unsafe.Pointer(&(pPolicy[solofsapitypes.MemBlockPlacementPolicyBodyOff]))) =
		solomqapitypes.TopicID(topicID)
}

func NetINodeBlockPlacementPolicyGetTopicID(
	pPolicy *solofsapitypes.MemBlockPlacementPolicy,
) solomqapitypes.TopicID {
	return (*(*solomqapitypes.TopicID)(unsafe.Pointer(&(pPolicy[solofsapitypes.MemBlockPlacementPolicyBodyOff]))))
}

func (p *TopicDriver) prepareNetBlockMetaDataWithRoleLeader(uTopic solomqapitypes.TopicUintptr,
	uNetBlock solofsapitypes.NetBlockUintptr,
	uNetINode solofsapitypes.NetINodeUintptr, netblockIndex int32) error {
	var (
		err               error
		pTopic                       = uTopic.Ptr()
		pNetBlock                    = uNetBlock.Ptr()
		queryNetJobsCount            = pTopic.Meta.SOLOMQMemberGroup.Len
		queryNetRetArr    chan error = make(chan error, queryNetJobsCount)
	)

	for _, solomqMember := range pTopic.Meta.SOLOMQMemberGroup.Slice() {
		go func(peerID snettypes.PeerID, uTopic solomqapitypes.TopicUintptr, queryNetRetArr chan error) {
			queryNetRetArr <- p.broker.brokerClient.PrepareTopicNetBlockMetaData(peerID,
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
		if pTopic.Meta.SOLOMQMemberGroup.Arr[i].PeerID == p.broker.srpcPeer.ID {
			pNetBlock.SyncDataBackends.Append(pNetBlock.StorDataBackends.Arr[i], 0)
		} else {
			pNetBlock.SyncDataBackends.Append(pTopic.Meta.SOLOMQMemberGroup.Arr[i].PeerID, 1)
			pNetBlock.SyncDataBackends.Append(pNetBlock.StorDataBackends.Arr[i], 0)
		}
	}

	return nil
}

func (p *TopicDriver) prepareNetBlockMetaDataWithRoleFollower(uTopic solomqapitypes.TopicUintptr,
	uNetBlock solofsapitypes.NetBlockUintptr,
	uNetINode solofsapitypes.NetINodeUintptr, netblockIndex int32) error {
	panic("fuck shit")
	return nil
}

func (p *TopicDriver) PrepareNetBlockMetaData(topicID solomqapitypes.TopicID,
	uNetBlock solofsapitypes.NetBlockUintptr,
	uNetINode solofsapitypes.NetINodeUintptr, netblockIndex int32) error {
	var uTopic, err = p.GetTopicByID(topicID)
	defer p.ReleaseTopic(uTopic)
	if err != nil {
		return err
	}

	NetINodeBlockPlacementPolicySetTopicID(&uNetINode.Ptr().MemBlockPlacementPolicy, topicID)

	switch p.computeTopicRole(uTopic) {
	case solomqapitypes.SOLOMQMemberRoleLeader:
		err = p.prepareNetBlockMetaDataWithRoleLeader(uTopic, uNetBlock, uNetINode, netblockIndex)
	case solomqapitypes.SOLOMQMemberRoleFollower:
		err = p.prepareNetBlockMetaDataWithRoleFollower(uTopic, uNetBlock, uNetINode, netblockIndex)
	}

	return nil
}

package agent

import (
	"soloos/common/sdfsapitypes"
	"soloos/common/snettypes"
	"soloos/common/swalapitypes"
	"unsafe"
)

func NetINodeBlockPlacementPolicySetTopicID(
	pPolicy *sdfsapitypes.MemBlockPlacementPolicy,
	topicID swalapitypes.TopicID,
) {
	*(*swalapitypes.TopicID)(unsafe.Pointer(&(pPolicy[sdfsapitypes.MemBlockPlacementPolicyBodyOff]))) =
		swalapitypes.TopicID(topicID)
}

func NetINodeBlockPlacementPolicyGetTopicID(
	pPolicy *sdfsapitypes.MemBlockPlacementPolicy,
) swalapitypes.TopicID {
	return (*(*swalapitypes.TopicID)(unsafe.Pointer(&(pPolicy[sdfsapitypes.MemBlockPlacementPolicyBodyOff]))))
}

func (p *TopicDriver) prepareNetBlockMetaDataWithRoleLeader(uTopic swalapitypes.TopicUintptr,
	uNetBlock sdfsapitypes.NetBlockUintptr,
	uNetINode sdfsapitypes.NetINodeUintptr, netblockIndex int32) error {
	var (
		err               error
		pTopic                       = uTopic.Ptr()
		pNetBlock                    = uNetBlock.Ptr()
		queryNetJobsCount            = pTopic.Meta.SWALMemberGroup.Len
		queryNetRetArr    chan error = make(chan error, queryNetJobsCount)
	)

	for _, swalMember := range pTopic.Meta.SWALMemberGroup.Slice() {
		go func(peerID snettypes.PeerID, uTopic swalapitypes.TopicUintptr, queryNetRetArr chan error) {
			queryNetRetArr <- p.swalAgent.swalAgentClient.PrepareTopicNetBlockMetaData(peerID,
				uTopic, uNetBlock, uNetINode, netblockIndex)
		}(swalMember.PeerID, uTopic, queryNetRetArr)
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
		if pTopic.Meta.SWALMemberGroup.Arr[i].PeerID == p.swalAgent.peer.ID {
			pNetBlock.SyncDataBackends.Append(pNetBlock.StorDataBackends.Arr[i], 0)
		} else {
			pNetBlock.SyncDataBackends.Append(pTopic.Meta.SWALMemberGroup.Arr[i].PeerID, 1)
			pNetBlock.SyncDataBackends.Append(pNetBlock.StorDataBackends.Arr[i], 0)
		}
	}

	return nil
}

func (p *TopicDriver) prepareNetBlockMetaDataWithRoleFollower(uTopic swalapitypes.TopicUintptr,
	uNetBlock sdfsapitypes.NetBlockUintptr,
	uNetINode sdfsapitypes.NetINodeUintptr, netblockIndex int32) error {
	panic("fuck shit")
	return nil
}

func (p *TopicDriver) PrepareNetBlockMetaData(topicID swalapitypes.TopicID,
	uNetBlock sdfsapitypes.NetBlockUintptr,
	uNetINode sdfsapitypes.NetINodeUintptr, netblockIndex int32) error {
	var uTopic, err = p.GetTopicByID(topicID)
	defer p.ReleaseTopic(uTopic)
	if err != nil {
		return err
	}

	NetINodeBlockPlacementPolicySetTopicID(&uNetINode.Ptr().MemBlockPlacementPolicy, topicID)

	switch p.computeTopicRole(uTopic) {
	case swalapitypes.SWALMemberRoleLeader:
		err = p.prepareNetBlockMetaDataWithRoleLeader(uTopic, uNetBlock, uNetINode, netblockIndex)
	case swalapitypes.SWALMemberRoleFollower:
		err = p.prepareNetBlockMetaDataWithRoleFollower(uTopic, uNetBlock, uNetINode, netblockIndex)
	}

	return nil
}

package agent

import (
	"path/filepath"
	"soloos/common/log"
	"soloos/common/sdfsapitypes"
	"soloos/common/snettypes"
	"soloos/common/swalapitypes"
	"unsafe"
)

func (p *TopicDriver) OpenFile(topicID swalapitypes.TopicID, path string) (sdfsapitypes.FsINodeFileHandlerID, error) {
	var (
		uTopic      swalapitypes.TopicUintptr
		pTopic      *swalapitypes.Topic
		fsINodeMeta sdfsapitypes.FsINodeMeta
		fdID        sdfsapitypes.FsINodeFileHandlerID
		dirPath     string
		err         error
	)

	uTopic, err = p.GetTopicByID(topicID)
	defer p.ReleaseTopic(uTopic)
	pTopic = uTopic.Ptr()
	if uTopic == 0 {
		log.Warn("get topic failed", err)
		return 0, err
	}

	dirPath = filepath.Dir(path)
	p.swalAgent.posixFS.SimpleMkdirAll(0777, dirPath, 0, 0)

	fsINodeMeta, err = p.swalAgent.posixFS.SimpleOpenFile(path,
		p.defaultNetBlockCap, p.defaultNetBlockCap)
	if err != nil {
		log.Error("open file failed", path, err)
		return 0, err
	}

	err = p.PrepareFsINodeMetaData(pTopic, &fsINodeMeta)
	if err != nil {
		return 0, err
	}

	fdID = p.swalAgent.posixFS.FdTableAllocFd(fsINodeMeta.Ino)

	return fdID, err
}

func (p *TopicDriver) NetINodeBlockPlacementPolicySetTopicID(
	pPolicy *sdfsapitypes.MemBlockPlacementPolicy,
	topicID swalapitypes.TopicID,
) {
	*(*swalapitypes.TopicID)(unsafe.Pointer(&(pPolicy[sdfsapitypes.MemBlockPlacementPolicyBodyOff]))) =
		swalapitypes.TopicID(topicID)
}

func (p *TopicDriver) NetINodeBlockPlacementPolicyGetTopicID(
	pPolicy *sdfsapitypes.MemBlockPlacementPolicy,
) swalapitypes.TopicID {
	return (*(*swalapitypes.TopicID)(unsafe.Pointer(&(pPolicy[sdfsapitypes.MemBlockPlacementPolicyBodyOff]))))
}

func (p *TopicDriver) PrepareFsINodeMetaData(
	pTopic *swalapitypes.Topic,
	pFsINodeMeta *sdfsapitypes.FsINodeMeta,
) error {
	var (
		policy sdfsapitypes.MemBlockPlacementPolicy
		err    error
	)

	err = p.swalAgent.SWALAgentClient.PrepareTopicFsINodeMetaData(pTopic, pFsINodeMeta)
	if err != nil {
		return err
	}

	policy.SetType(sdfsapitypes.BlockPlacementPolicySWAL)
	p.NetINodeBlockPlacementPolicySetTopicID(&policy, pTopic.ID)

	err = p.swalAgent.posixFS.SetNetINodeBlockPlacement(pFsINodeMeta.NetINodeID, policy)
	if err != nil {
		return err
	}

	return nil
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
			queryNetRetArr <- p.swalAgent.SWALAgentClient.PrepareTopicNetBlockMetaData(peerID,
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

	log.Error("\n\nSyncDataBackends")
	for _, backend := range pNetBlock.SyncDataBackends.Slice() {
		log.Error(backend.PeerID.Str(), backend.TransferCount)
	}
	log.Error(pNetBlock.IsLocalDataBackendExists)

	return nil
}

func (p *TopicDriver) prepareNetBlockMetaDataWithRoleFollower(uTopic swalapitypes.TopicUintptr,
	uNetBlock sdfsapitypes.NetBlockUintptr,
	uNetINode sdfsapitypes.NetINodeUintptr, netblockIndex int32) error {
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

	p.NetINodeBlockPlacementPolicySetTopicID(&uNetINode.Ptr().MemBlockPlacementPolicy, topicID)

	log.Error(uTopic.Ptr().Meta.TopicID)
	switch p.computeTopicRole(uTopic) {
	case swalapitypes.SWALMemberRoleLeader:
		log.Error(uTopic.Ptr().Meta.TopicName.Str(), "prepareNetBlockMetaDataWithRoleLeader")
		err = p.prepareNetBlockMetaDataWithRoleLeader(uTopic, uNetBlock, uNetINode, netblockIndex)
	case swalapitypes.SWALMemberRoleFollower:
		log.Error(uTopic.Ptr().Meta.TopicName.Str(), "prepareNetBlockMetaDataWithRoleFollower")
		err = p.prepareNetBlockMetaDataWithRoleFollower(uTopic, uNetBlock, uNetINode, netblockIndex)
	}

	return nil
}

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
	p.FsINodeBlockPlacementPolicySetTopic(pTopic, &policy)

	err = p.swalAgent.posixFS.SetFsINodeBlockPlacement(pFsINodeMeta.Ino, policy)
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

	for _, backend := range pNetBlock.StorDataBackends.Slice() {
		log.Error(backend.Ptr().PeerIDStr())
		log.Error(backend.Ptr().AddressStr())
	}

	log.Error("\n\nSyncDataBackends", pNetBlock.SyncDataPrimaryBackendTransferCount)
	for _, backend := range pNetBlock.SyncDataBackends.Slice() {
		log.Error(backend.Ptr().PeerIDStr())
		log.Error(backend.Ptr().AddressStr())
	}
	log.Error(pNetBlock.LocalDataBackend)
	panic("fuck")

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
	var uTopic, err = p.FsINodeBlockPlacementPolicyGetTopic(&uNetINode.Ptr().MemBlockPlacementPolicy)
	if err != nil {
		return err
	}

	log.Error(uTopic.Ptr().Meta.TopicID)
	log.Error(uTopic.Ptr().Meta.TopicName.Str())
	switch p.computeTopicRole(uTopic) {
	case swalapitypes.SWALMemberRoleLeader:
		err = p.prepareNetBlockMetaDataWithRoleLeader(uTopic, uNetBlock, uNetINode, netblockIndex)
	case swalapitypes.SWALMemberRoleFollower:
		err = p.prepareNetBlockMetaDataWithRoleFollower(uTopic, uNetBlock, uNetINode, netblockIndex)
	}

	return nil
}

func (p *TopicDriver) FsINodeBlockPlacementPolicySetTopic(
	pTopic *swalapitypes.Topic,
	pPolicy *sdfsapitypes.MemBlockPlacementPolicy,
) {
	*(*swalapitypes.TopicID)(unsafe.Pointer(&(pPolicy[sdfsapitypes.MemBlockPlacementPolicyHeaderBytesNum]))) =
		swalapitypes.TopicID(pTopic.Meta.TopicID)
}

func (p *TopicDriver) FsINodeBlockPlacementPolicyGetTopic(
	pPolicy *sdfsapitypes.MemBlockPlacementPolicy,
) (swalapitypes.TopicUintptr, error) {
	var topicID = (*(*swalapitypes.TopicID)(unsafe.Pointer(&(pPolicy[sdfsapitypes.MemBlockPlacementPolicyHeaderBytesNum]))))
	return p.GetTopicByID(topicID)
}

package broker

import (
	"path/filepath"
	"soloos/common/log"
	"soloos/common/sdfsapitypes"
	"soloos/common/snettypes"
	"soloos/common/swalapitypes"
)

func (p *TopicDriver) OpenFile(topicID swalapitypes.TopicID, path string) (sdfsapitypes.FsINodeFileHandlerID, error) {
	var (
		uTopic      swalapitypes.TopicUintptr
		fsINodeMeta sdfsapitypes.FsINodeMeta
		fdID        sdfsapitypes.FsINodeFileHandlerID
		dirPath     string
		err         error
	)

	uTopic, err = p.GetTopicByID(topicID)
	defer p.ReleaseTopic(uTopic)
	if uTopic == 0 {
		log.Warn("get topic failed", err)
		return 0, err
	}

	dirPath = filepath.Dir(path)
	p.broker.posixFS.SimpleMkdirAll(0777, dirPath, 0, 0)

	fsINodeMeta, err = p.broker.posixFS.SimpleOpenFile(path,
		p.defaultNetBlockCap, p.defaultNetBlockCap)
	if err != nil {
		log.Error("open file failed", path, err)
		return 0, err
	}

	err = p.PrepareTopicMetaData(uTopic, &fsINodeMeta)
	if err != nil {
		return 0, err
	}

	fdID = p.broker.posixFS.FdTableAllocFd(fsINodeMeta.Ino)

	return fdID, err
}

func (p *TopicDriver) PrepareTopicMetaData(
	uTopic swalapitypes.TopicUintptr,
	pFsINodeMeta *sdfsapitypes.FsINodeMeta,
) error {
	var (
		policy sdfsapitypes.MemBlockPlacementPolicy
		pTopic = uTopic.Ptr()
		jobNum int
		jobRet chan error
		i      int
		err    error
	)

	jobNum = 0
	for i, _ = range pTopic.Meta.SWALMemberGroup.Slice() {
		if pTopic.Meta.SWALMemberGroup.Arr[i].PeerID == p.broker.srpcPeer.ID {
			continue
		}
		jobNum++
	}
	jobRet = make(chan error, jobNum)

	for i, _ = range pTopic.Meta.SWALMemberGroup.Slice() {
		go func(jobRet chan error, index int,
			peerID snettypes.PeerID, uTopic swalapitypes.TopicUintptr, fsINodeID sdfsapitypes.FsINodeID) {
			jobRet <- p.broker.brokerClient.PrepareTopicMetaData(
				uTopic.Ptr().Meta.SWALMemberGroup.Arr[index].PeerID,
				uTopic, fsINodeID)
		}(jobRet, i, pTopic.Meta.SWALMemberGroup.Arr[i].PeerID, uTopic, pFsINodeMeta.Ino)
	}

	{
		var tmpErr error
		for i = 0; i < jobNum; i++ {
			tmpErr = <-jobRet
			if tmpErr != nil {
				err = tmpErr
			}
		}
	}
	if err != nil {
		return err
	}

	policy.SetType(sdfsapitypes.BlockPlacementPolicySWAL)
	NetINodeBlockPlacementPolicySetTopicID(&policy, pTopic.ID)

	err = p.broker.posixFS.SetNetINodeBlockPlacement(pFsINodeMeta.NetINodeID, policy)
	if err != nil {
		return err
	}

	return nil
}

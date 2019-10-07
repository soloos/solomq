package solomq

import (
	"path/filepath"
	"soloos/common/log"
	"soloos/common/snet"
	"soloos/common/solofstypes"
	"soloos/common/solomqtypes"
)

func (p *TopicDriver) OpenFile(topicID solomqtypes.TopicID, path string) (solofstypes.FsINodeFileHandlerID, error) {
	var (
		uTopic      solomqtypes.TopicUintptr
		fsINodeMeta solofstypes.FsINodeMeta
		fdID        solofstypes.FsINodeFileHandlerID
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
	p.solomq.posixFs.SimpleMkdirAll(0777, dirPath, 0, 0)

	fsINodeMeta, err = p.solomq.posixFs.SimpleOpenFile(path,
		p.defaultNetBlockCap, p.defaultNetBlockCap)
	if err != nil {
		log.Error("open file failed", path, err)
		return 0, err
	}

	err = p.PrepareTopicMetaData(uTopic, &fsINodeMeta)
	if err != nil {
		return 0, err
	}

	fdID = p.solomq.posixFs.FdTableAllocFd(fsINodeMeta.Ino)

	return fdID, err
}

func (p *TopicDriver) PrepareTopicMetaData(
	uTopic solomqtypes.TopicUintptr,
	pFsINodeMeta *solofstypes.FsINodeMeta,
) error {
	var (
		policy solofstypes.MemBlockPlacementPolicy
		pTopic = uTopic.Ptr()
		jobNum int
		jobRet chan error
		i      int
		err    error
	)

	jobNum = 0
	for i, _ = range pTopic.Meta.SolomqMemberGroup.Slice() {
		if pTopic.Meta.SolomqMemberGroup.Arr[i].PeerID == p.solomq.srpcPeer.ID {
			continue
		}
		jobNum++
	}
	jobRet = make(chan error, jobNum)

	for i, _ = range pTopic.Meta.SolomqMemberGroup.Slice() {
		go func(jobRet chan error, index int,
			peerID snet.PeerID, uTopic solomqtypes.TopicUintptr, fsINodeID solofstypes.FsINodeID) {
			jobRet <- p.solomq.PrepareTopicMetaDataToNet(
				uTopic.Ptr().Meta.SolomqMemberGroup.Arr[index].PeerID,
				uTopic, fsINodeID)
		}(jobRet, i, pTopic.Meta.SolomqMemberGroup.Arr[i].PeerID, uTopic, pFsINodeMeta.Ino)
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

	policy.SetType(solofstypes.BlockPlacementPolicySolomq)
	NetINodeBlockPlacementPolicySetTopicID(&policy, pTopic.ID)

	err = p.solomq.posixFs.SetNetINodeBlockPlacement(pFsINodeMeta.NetINodeID, policy)
	if err != nil {
		return err
	}

	return nil
}

package solomq

import (
	"soloos/common/solofstypes"
	"soloos/common/solomqtypes"
)

func (p *Solomq) OpenTopicFile(topicID solomqtypes.TopicID, path string) (solofstypes.FsINodeFileHandlerID, error) {
	return p.TopicDriver.OpenFile(topicID, path)
}

func (p *Solomq) PrepareNetBlockMetaData(topicID solomqtypes.TopicID,
	uNetBlock solofstypes.NetBlockUintptr,
	uNetINode solofstypes.NetINodeUintptr, netblockIndex int32) error {
	return p.TopicDriver.PrepareNetBlockMetaData(topicID,
		uNetBlock, uNetINode, netblockIndex)
}

func (p *Solomq) UploadMemBlockWithSolomq(uJob solofstypes.UploadMemBlockJobUintptr,
	uploadPeerIndex int) error {
	return p.TopicDriver.UploadMemBlockWithSolomq(uJob, uploadPeerIndex)
}

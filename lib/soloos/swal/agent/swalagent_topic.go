package agent

import (
	"soloos/common/sdfsapitypes"
	"soloos/common/swalapitypes"
)

func (p *SWALAgent) OpenTopicFile(topicID swalapitypes.TopicID, path string) (sdfsapitypes.FsINodeFileHandlerID, error) {
	return p.TopicDriver.OpenFile(topicID, path)
}

func (p *SWALAgent) PrepareNetBlockMetaData(topicID swalapitypes.TopicID,
	uNetBlock sdfsapitypes.NetBlockUintptr,
	uNetINode sdfsapitypes.NetINodeUintptr, netblockIndex int32) error {
	return p.TopicDriver.PrepareNetBlockMetaData(topicID,
		uNetBlock, uNetINode, netblockIndex)
}

func (p *SWALAgent) UploadMemBlockWithSWAL(uJob sdfsapitypes.UploadMemBlockJobUintptr,
	uploadPeerIndex int) error {
	return p.TopicDriver.UploadMemBlockWithSWAL(uJob, uploadPeerIndex)
}

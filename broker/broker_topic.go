package broker

import (
	"soloos/common/solofsapitypes"
	"soloos/common/solomqapitypes"
)

func (p *Broker) OpenTopicFile(topicID solomqapitypes.TopicID, path string) (solofsapitypes.FsINodeFileHandlerID, error) {
	return p.TopicDriver.OpenFile(topicID, path)
}

func (p *Broker) PrepareNetBlockMetaData(topicID solomqapitypes.TopicID,
	uNetBlock solofsapitypes.NetBlockUintptr,
	uNetINode solofsapitypes.NetINodeUintptr, netblockIndex int32) error {
	return p.TopicDriver.PrepareNetBlockMetaData(topicID,
		uNetBlock, uNetINode, netblockIndex)
}

func (p *Broker) UploadMemBlockWithSOLOMQ(uJob solofsapitypes.UploadMemBlockJobUintptr,
	uploadPeerIndex int) error {
	return p.TopicDriver.UploadMemBlockWithSOLOMQ(uJob, uploadPeerIndex)
}

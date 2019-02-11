package agent

import (
	"soloos/common/log"
	"soloos/common/sdfsapitypes"
	"soloos/common/snettypes"
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

func (p *SWALAgent) TopicSendMsg(pTopic *swalapitypes.Topic, msg []byte) error {
	var swalMember swalapitypes.SWALMember
	log.Info("fuck peer", p.GetPeerID().Str())
	for _, swalMember = range pTopic.Meta.SWALMemberGroup.Slice() {
		log.Info("fuck send peer", swalMember.PeerID.Str(), swalMember.Role)
	}
	log.Info("fuck send msg", msg)

	return nil
}

func (p *SWALAgent) TopicConsumeMsg(pTopic *swalapitypes.Topic, netQuery *snettypes.NetQuery) error {
	return nil
}

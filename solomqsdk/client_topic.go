package solomqsdk

import (
	"soloos/common/solofsapitypes"
	"soloos/common/solomqapitypes"
)

func (p *Client) initTopic(topicName string, solomqMembers []solomqapitypes.SOLOMQMember) error {
	var err error
	p.uTopic, err = p.clientDriver.broker.MustGetTopic(topicName, solomqMembers)
	if err != nil {
		return err
	}

	return err
}

func (p *Client) SendMsg(msg []byte) error {
	return nil
}

func (p *Client) OpenTopicFile(path string) (solofsapitypes.FsINodeFileHandlerID, error) {
	return p.clientDriver.broker.OpenTopicFile(p.uTopic.Ptr().Meta.TopicID, path)
}

func (p *Client) PrepareNetBlockMetaData(uNetBlock solofsapitypes.NetBlockUintptr,
	uNetINode solofsapitypes.NetINodeUintptr, netblockIndex int32) error {
	return p.clientDriver.broker.PrepareNetBlockMetaData(p.uTopic.Ptr().Meta.TopicID,
		uNetBlock, uNetINode, netblockIndex)
}

func (p *Client) UploadMemBlockWithSOLOMQ(uJob solofsapitypes.UploadMemBlockJobUintptr,
	uploadPeerIndex int) error {
	return p.clientDriver.broker.UploadMemBlockWithSOLOMQ(uJob,
		uploadPeerIndex)
}

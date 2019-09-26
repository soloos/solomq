package solomqsdk

import (
	"soloos/common/solofsapitypes"
	"soloos/common/solomqapitypes"
)

func (p *Client) initTopic(topicName string, solomqMembers []solomqapitypes.SolomqMember) error {
	var err error
	p.uTopic, err = p.clientDriver.solomq.MustGetTopic(topicName, solomqMembers)
	if err != nil {
		return err
	}

	return err
}

func (p *Client) SendMsg(msg []byte) error {
	return nil
}

func (p *Client) OpenTopicFile(path string) (solofsapitypes.FsINodeFileHandlerID, error) {
	return p.clientDriver.solomq.OpenTopicFile(p.uTopic.Ptr().Meta.TopicID, path)
}

func (p *Client) PrepareNetBlockMetaData(uNetBlock solofsapitypes.NetBlockUintptr,
	uNetINode solofsapitypes.NetINodeUintptr, netblockIndex int32) error {
	return p.clientDriver.solomq.PrepareNetBlockMetaData(p.uTopic.Ptr().Meta.TopicID,
		uNetBlock, uNetINode, netblockIndex)
}

func (p *Client) UploadMemBlockWithSolomq(uJob solofsapitypes.UploadMemBlockJobUintptr,
	uploadPeerIndex int) error {
	return p.clientDriver.solomq.UploadMemBlockWithSolomq(uJob,
		uploadPeerIndex)
}

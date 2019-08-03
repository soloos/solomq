package swalsdk

import (
	"soloos/common/sdfsapitypes"
	"soloos/common/swalapitypes"
)

func (p *Client) initTopic(topicName string, swalMembers []swalapitypes.SWALMember) error {
	var err error
	p.uTopic, err = p.clientDriver.broker.MustGetTopic(topicName, swalMembers)
	if err != nil {
		return err
	}

	return err
}

func (p *Client) SendMsg(msg []byte) error {
	return nil
}

func (p *Client) OpenTopicFile(path string) (sdfsapitypes.FsINodeFileHandlerID, error) {
	return p.clientDriver.broker.OpenTopicFile(p.uTopic.Ptr().Meta.TopicID, path)
}

func (p *Client) PrepareNetBlockMetaData(uNetBlock sdfsapitypes.NetBlockUintptr,
	uNetINode sdfsapitypes.NetINodeUintptr, netblockIndex int32) error {
	return p.clientDriver.broker.PrepareNetBlockMetaData(p.uTopic.Ptr().Meta.TopicID,
		uNetBlock, uNetINode, netblockIndex)
}

func (p *Client) UploadMemBlockWithSWAL(uJob sdfsapitypes.UploadMemBlockJobUintptr,
	uploadPeerIndex int) error {
	return p.clientDriver.broker.UploadMemBlockWithSWAL(uJob,
		uploadPeerIndex)
}

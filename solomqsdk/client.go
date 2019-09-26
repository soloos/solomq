package solomqsdk

import (
	"soloos/common/solofsapi"
	"soloos/common/solomqapi"
	"soloos/common/solomqapitypes"
)

type Client struct {
	clientDriver *ClientDriver
	uTopic       solomqapitypes.TopicUintptr

	solofsClient solofsapi.Client
}

var _ = solomqapi.Client(&Client{})

func (p *Client) Init(clientDriver *ClientDriver,
	topicName string, solomqMembers []solomqapitypes.SOLOMQMember,
) error {
	var err error
	p.clientDriver = clientDriver
	err = p.initTopic(topicName, solomqMembers)
	if err != nil {
		return err
	}

	return nil
}

func (p *Client) Close() error {
	return nil
}

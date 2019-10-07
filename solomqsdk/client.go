package solomqsdk

import (
	"soloos/common/solofsapi"
	"soloos/common/solomqapi"
	"soloos/common/solomqtypes"
)

type Client struct {
	clientDriver *ClientDriver
	uTopic       solomqtypes.TopicUintptr

	solofsClient solofsapi.Client
}

var _ = solomqapi.Client(&Client{})

func (p *Client) Init(clientDriver *ClientDriver,
	topicName string, solomqMembers []solomqtypes.SolomqMember,
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

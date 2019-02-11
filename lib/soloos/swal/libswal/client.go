package libswal

import (
	"soloos/common/sdfsapi"
	"soloos/common/swalapi"
	"soloos/common/swalapitypes"
)

type Client struct {
	clientDriver *ClientDriver
	uTopic       swalapitypes.TopicUintptr

	sdfsClient sdfsapi.Client
}

var _ = swalapi.Client(&Client{})

func (p *Client) Init(clientDriver *ClientDriver,
	topicName string, swalMembers []swalapitypes.SWALMember,
) error {
	var err error
	p.clientDriver = clientDriver
	err = p.initTopic(topicName, swalMembers)
	if err != nil {
		return err
	}

	return nil
}

func (p *Client) Close() error {
	return nil
}

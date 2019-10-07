package solomq

import (
	"soloos/common/snet"
	"soloos/common/solomqprotocol"
)

func (p *SrpcServer) ctrTopicPrepare(
	reqCtx *snet.SNetReqContext,
	req solomqprotocol.TopicPrepareReq,
) error {
	// TODO should prepare topic
	return nil
}

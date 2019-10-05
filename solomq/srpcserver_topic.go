package solomq

import (
	"soloos/common/snettypes"
	"soloos/common/solomqprotocol"
)

func (p *SrpcServer) ctrTopicPrepare(
	reqCtx *snettypes.SNetReqContext,
	req solomqprotocol.TopicPrepareReq,
) error {
	// TODO should prepare topic
	return nil
}

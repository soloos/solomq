package broker

import (
	"soloos/common/solofsapi"
	"soloos/common/snettypes"
	"soloos/common/solomqprotocol"

	flatbuffers "github.com/google/flatbuffers/go"
)

func (p *SRPCServer) ctrTopicPrepare(serviceReq *snettypes.NetQuery) error {
	var (
		reqParamData = make([]byte, serviceReq.ParamSize)
		reqParam     solomqprotocol.TopicPrepareRequest
		err          error
	)

	// request param
	err = serviceReq.ReadAll(reqParamData)
	if err != nil {
		return err
	}
	reqParam.Init(reqParamData[:serviceReq.ParamSize], flatbuffers.GetUOffsetT(reqParamData[:serviceReq.ParamSize]))

	// response
	var protocolBuilder flatbuffers.Builder

	// get uNetINode
	// TODO should prepare topic
	goto SERVICE_DONE

SERVICE_DONE:
	if err != nil {
		return nil
	}

	if err == nil {
		solofsapi.SetCommonResponseCode(&protocolBuilder, snettypes.CODE_OK)
	}

	respBody := protocolBuilder.Bytes[protocolBuilder.Head():]
	err = serviceReq.SimpleResponse(serviceReq.ReqID, respBody)
	if err != nil {
		return err
	}

	return nil
}

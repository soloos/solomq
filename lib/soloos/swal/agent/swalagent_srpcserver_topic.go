package agent

import (
	"soloos/common/log"
	"soloos/common/sdfsapitypes"
	"soloos/common/snettypes"
	"soloos/common/swalprotocol"

	flatbuffers "github.com/google/flatbuffers/go"
)

func (p *SWALAgentSRPCServer) ctrTopicPrepare(serviceReq *snettypes.NetQuery) error {
	return nil
}

func (p *SWALAgentSRPCServer) ctrTopicPWrite(serviceReq *snettypes.NetQuery) error {
	var (
		reqParamData = make([]byte, serviceReq.ParamSize)
		reqParam     swalprotocol.TopicPWriteRequest
		err          error
	)
	log.Error("fuck shit")

	// request param
	err = serviceReq.ReadAll(reqParamData)
	if err != nil {
		return err
	}
	reqParam.Init(reqParamData[:serviceReq.ParamSize], flatbuffers.GetUOffsetT(reqParamData[:serviceReq.ParamSize]))

	// response

	// get uNetINode
	var (
		netINodeID sdfsapitypes.NetINodeID
	)
	copy(netINodeID[:], reqParam.NetINodeID())
	log.Error(netINodeID.Str())
	log.Error(reqParam.TopicID())
	log.Error(reqParam.Length())
	log.Error(reqParam.Offset())
	{
		var offset = reqParam.Offset()
		var length = reqParam.Length()
		var bytes = make([]byte, length)
		serviceReq.ReadAll(bytes[offset : offset+uint64(length)])
		log.Error("fuck", string(bytes))
	}
	panic("fuck")

	return nil
}

package solomq

import (
	"soloos/common/solodbapitypes"
	"soloos/common/solofsapi"
	"soloos/common/solofsapitypes"
	"soloos/common/snettypes"
	"soloos/common/solomqprotocol"

	flatbuffers "github.com/google/flatbuffers/go"
)

func (p *SRPCServer) ctrTopicPWrite(serviceReq *snettypes.NetQuery) error {
	var (
		reqParamData     = make([]byte, serviceReq.ParamSize)
		reqParam         solomqprotocol.TopicPWriteRequest
		syncDataBackends snettypes.PeerGroup
		peerID           snettypes.PeerID
		uNetBlock        solofsapitypes.NetBlockUintptr
		i                int
		err              error
	)

	// request param
	err = serviceReq.ReadAll(reqParamData)
	if err != nil {
		return err
	}
	reqParam.Init(reqParamData[:serviceReq.ParamSize], flatbuffers.GetUOffsetT(reqParamData[:serviceReq.ParamSize]))

	// response

	// get uNetINode
	var (
		protocolBuilder    flatbuffers.Builder
		netINodeID         solofsapitypes.NetINodeID
		uNetINode          solofsapitypes.NetINodeUintptr
		firstNetBlockIndex int32
		lastNetBlockIndex  int32
		netBlockIndex      int32
	)
	copy(netINodeID[:], reqParam.NetINodeID())

	uNetINode, err = p.solomq.posixFS.GetNetINode(netINodeID)
	defer p.solomq.posixFS.ReleaseNetINode(uNetINode)
	if err != nil {
		if err == solofsapitypes.ErrObjectNotExists {
			solofsapi.SetCommonResponseCode(&protocolBuilder, snettypes.CODE_404)
			goto SERVICE_DONE
		} else {
			solofsapi.SetCommonResponseCode(&protocolBuilder, snettypes.CODE_502)
			goto SERVICE_DONE
		}
	}

	// TODO no need prepare syncDataBackends every pwrite
	syncDataBackends.Reset()
	syncDataBackends.Append(p.solomq.localFsSNetPeer.ID)
	for i = 0; i < reqParam.TransferBackendsLength(); i++ {
		copy(peerID[:], reqParam.TransferBackends(i))
		syncDataBackends.Append(peerID)
	}

	// prepare uNetBlock
	firstNetBlockIndex = int32(reqParam.Offset() / uint64(uNetINode.Ptr().NetBlockCap))
	lastNetBlockIndex = int32((reqParam.Offset() + uint64(reqParam.Length())) / uint64(uNetINode.Ptr().NetBlockCap))
	for netBlockIndex = firstNetBlockIndex; netBlockIndex <= lastNetBlockIndex; netBlockIndex++ {
		uNetBlock, err = p.solomq.posixFS.MustGetNetBlock(uNetINode, netBlockIndex)
		defer p.solomq.posixFS.ReleaseNetBlock(uNetBlock)
		if err != nil {
			solofsapi.SetCommonResponseCode(&protocolBuilder, snettypes.CODE_502)
			goto SERVICE_DONE
		}

		if uNetBlock.Ptr().IsSyncDataBackendsInited.Load() == solodbapitypes.MetaDataStateUninited {
			p.solomq.PrepareNetBlockSyncDataBackends(uNetBlock, syncDataBackends)
		}
	}

	// request file data
	err = p.solomq.posixFS.NetINodePWriteWithNetQuery(uNetINode, serviceReq,
		int(reqParam.Length()), reqParam.Offset())
	if err != nil {
		return err
	}

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

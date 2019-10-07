package solomq

import (
	"soloos/common/snet"
	"soloos/common/solodbapitypes"
	"soloos/common/solofsapitypes"
	"soloos/common/solomqprotocol"
)

func (p *SrpcServer) ctrTopicPWrite(
	reqCtx *snet.SNetReqContext,
	req solomqprotocol.TopicPWriteReq,
) error {
	var (
		syncDataBackends snet.PeerGroup
		peerID           snet.PeerID
		uNetBlock        solofsapitypes.NetBlockUintptr
		i                int
		err              error
	)

	// response

	// get uNetINode
	var (
		netINodeID         solofsapitypes.NetINodeID
		uNetINode          solofsapitypes.NetINodeUintptr
		firstNetBlockIndex int32
		lastNetBlockIndex  int32
		netBlockIndex      int32
	)
	netINodeID = req.NetINodeID

	uNetINode, err = p.solomq.posixFs.GetNetINode(netINodeID)
	defer p.solomq.posixFs.ReleaseNetINode(uNetINode)
	if err != nil {
		return err
	}

	// TODO no need prepare syncDataBackends every pwrite
	syncDataBackends.Reset()
	syncDataBackends.Append(p.solomq.localFsSNetPeer.ID)
	for i, _ = range req.TransferBackends {
		peerID.SetStr(req.TransferBackends[i])
		syncDataBackends.Append(peerID)
	}

	// prepare uNetBlock
	firstNetBlockIndex = int32(req.Offset / uint64(uNetINode.Ptr().NetBlockCap))
	lastNetBlockIndex = int32((req.Offset + uint64(req.Length)) / uint64(uNetINode.Ptr().NetBlockCap))
	for netBlockIndex = firstNetBlockIndex; netBlockIndex <= lastNetBlockIndex; netBlockIndex++ {
		uNetBlock, err = p.solomq.posixFs.MustGetNetBlock(uNetINode, netBlockIndex)
		defer p.solomq.posixFs.ReleaseNetBlock(uNetBlock)
		if err != nil {
			return err
		}

		if uNetBlock.Ptr().IsSyncDataBackendsInited.Load() == solodbapitypes.MetaDataStateUninited {
			p.solomq.PrepareNetBlockSyncDataBackends(uNetBlock, syncDataBackends)
		}
	}

	// request file data
	err = p.solomq.posixFs.NetINodePWriteWithNetQuery(uNetINode, &reqCtx.NetQuery,
		int(req.Length), req.Offset)
	if err != nil {
		return err
	}

	return nil
}

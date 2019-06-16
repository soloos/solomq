package agent

import (
	"soloos/common/log"
	"soloos/common/sdfsapitypes"
	"soloos/common/sdfsprotocol"
	"soloos/common/snettypes"
	"soloos/common/swalprotocol"
	"soloos/sdbone/offheap"

	flatbuffers "github.com/google/flatbuffers/go"
)

func (p *TopicDriver) UploadMemBlockWithSWAL(uJob sdfsapitypes.UploadMemBlockJobUintptr,
	uploadPeerIndex int) error {
	var pNetINode = uJob.Ptr().UNetINode.Ptr()
	var pNetBlock = uJob.Ptr().UNetBlock.Ptr()
	var topicID = p.NetINodeBlockPlacementPolicyGetTopicID(&pNetINode.MemBlockPlacementPolicy)
	var uTopic, err = p.GetTopicByID(topicID)
	defer p.ReleaseTopic(uTopic)
	if err != nil {
		return err
	}
	var pTopic = uTopic.Ptr()

	log.Error("fuck start", p.swalAgent.peer.PeerIDStr(), pTopic.Meta.TopicName.Str())
	var peer = pNetBlock.SyncDataBackends.Arr[uploadPeerIndex]
	log.Error("fuck sync", peer.PeerID.Str(), peer.TransferCount)
	peer = pNetBlock.SyncDataBackends.Arr[uploadPeerIndex+int(peer.TransferCount)]
	log.Error("fuck sync", peer.PeerID.Str(), peer.TransferCount)

	var (
		req                 snettypes.Request
		resp                snettypes.Response
		protocolBuilder     flatbuffers.Builder
		netINodeIDOff       flatbuffers.UOffsetT
		backendOff          flatbuffers.UOffsetT
		transferPeersCount  int
		netINodeWriteOffset int
		netINodeWriteLength int
		memBlockCap         int
		backendOffs         = make([]flatbuffers.UOffsetT, 8)
		uploadChunkMask     offheap.ChunkMask
		commonResp          swalprotocol.CommonResponse
		respBody            []byte
		i                   int
		backendPeer         snettypes.Peer
	)

	var soloOSEnv = p.swalAgent.SoloOSEnv
	var pJob = uJob.Ptr()
	var pMemBlock = pJob.UMemBlock.Ptr()
	uploadChunkMask = pJob.GetProcessingChunkMask()
	transferPeersCount = int(pNetBlock.SyncDataBackends.Arr[uploadPeerIndex].TransferCount)

	req.OffheapBody.OffheapBytes = pMemBlock.Bytes.Data
	memBlockCap = pMemBlock.Bytes.Len
	for chunkMaskIndex := 0; chunkMaskIndex < uploadChunkMask.MaskArrayLen; chunkMaskIndex++ {
		req.OffheapBody.CopyOffset = uploadChunkMask.MaskArray[chunkMaskIndex].Offset
		req.OffheapBody.CopyEnd = uploadChunkMask.MaskArray[chunkMaskIndex].End
		netINodeWriteOffset = memBlockCap*int(pJob.MemBlockIndex) + req.OffheapBody.CopyOffset
		netINodeWriteLength = req.OffheapBody.CopyEnd - req.OffheapBody.CopyOffset

		if transferPeersCount > 0 {
			for i = 0; i < transferPeersCount; i++ {
				backendPeer, _ = soloOSEnv.SNetDriver.GetPeer(pNetBlock.SyncDataBackends.Arr[uploadPeerIndex+1+i].PeerID)
				if i < cap(backendOffs) {
					backendOffs[i] = protocolBuilder.CreateString(backendPeer.PeerIDStr())
				} else {
					backendOffs = append(backendOffs, protocolBuilder.CreateString(backendPeer.PeerIDStr()))
				}
			}

			sdfsprotocol.NetINodePWriteRequestStartTransferBackendsVector(&protocolBuilder, len(backendOffs))
			for i = len(backendOffs) - 1; i >= 0; i-- {
				protocolBuilder.PrependUOffsetT(backendOffs[i])
			}
			backendOff = protocolBuilder.EndVector(len(backendOffs))
		}

		netINodeIDOff = protocolBuilder.CreateByteVector(pNetBlock.NetINodeID[:])
		swalprotocol.TopicPWriteRequestStart(&protocolBuilder)
		if transferPeersCount > 0 {
			swalprotocol.TopicPWriteRequestAddTransferBackends(&protocolBuilder, backendOff)
		}
		swalprotocol.TopicPWriteRequestAddTopicID(&protocolBuilder, pTopic.ID)
		swalprotocol.TopicPWriteRequestAddNetINodeID(&protocolBuilder, netINodeIDOff)
		swalprotocol.TopicPWriteRequestAddOffset(&protocolBuilder, uint64(netINodeWriteOffset))
		swalprotocol.TopicPWriteRequestAddLength(&protocolBuilder, int32(netINodeWriteLength))
		protocolBuilder.Finish(swalprotocol.TopicPWriteRequestEnd(&protocolBuilder))
		req.Param = protocolBuilder.Bytes[protocolBuilder.Head():]

		backendPeer, err = soloOSEnv.SNetDriver.GetPeer(pNetBlock.SyncDataBackends.Arr[uploadPeerIndex].PeerID)
		if err != nil {
			goto PWRITE_DONE
		}

		err = soloOSEnv.SNetClientDriver.Call(backendPeer.ID,
			"/Topic/PWrite", &req, &resp)
		if err != nil {
			goto PWRITE_DONE
		}

		respBody = make([]byte, resp.ParamSize)
		err = soloOSEnv.SNetClientDriver.ReadResponse(backendPeer.ID, &req, &resp, respBody)
		if err != nil {
			goto PWRITE_DONE
		}
		commonResp.Init(respBody[:(resp.ParamSize)], flatbuffers.GetUOffsetT(respBody[:(resp.ParamSize)]))
		if commonResp.Code() != snettypes.CODE_OK {
			err = sdfsapitypes.ErrNetBlockPWrite
			goto PWRITE_DONE
		}
		protocolBuilder.Reset()
	}

PWRITE_DONE:
	return err
}

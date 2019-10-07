package solomq

import (
	"soloos/common/snet"
	"soloos/common/solodbtypes"
	"soloos/common/solofstypes"
)

func (p *Solomq) doPrepareNetBlockSyncDataBackendsWithFanout(uNetBlock solofstypes.NetBlockUintptr,
	backends snet.PeerGroup,
) error {
	var (
		pNetBlock = uNetBlock.Ptr()
		err       error
	)

	pNetBlock.IsSyncDataBackendsInited.LockContext()
	if pNetBlock.IsSyncDataBackendsInited.Load() == solodbtypes.MetaDataStateInited {
		goto PREPARE_DONE
	}

	// fanout
	pNetBlock.SyncDataBackends.Reset()
	for i, _ := range backends.Slice() {
		pNetBlock.SyncDataBackends.Append(backends.Arr[i], 0)
	}
	pNetBlock.IsSyncDataBackendsInited.Store(solodbtypes.MetaDataStateInited)

PREPARE_DONE:
	pNetBlock.IsSyncDataBackendsInited.UnlockContext()
	return err
}

func (p *Solomq) PrepareNetBlockSyncDataBackends(uNetBlock solofstypes.NetBlockUintptr,
	syncDataBackends snet.PeerGroup) error {
	return p.doPrepareNetBlockSyncDataBackendsWithFanout(uNetBlock, syncDataBackends)
}

package solomq

import (
	"soloos/common/snettypes"
	"soloos/common/solodbapitypes"
	"soloos/common/solofsapitypes"
)

func (p *Solomq) doPrepareNetBlockSyncDataBackendsWithFanout(uNetBlock solofsapitypes.NetBlockUintptr,
	backends snettypes.PeerGroup,
) error {
	var (
		pNetBlock = uNetBlock.Ptr()
		err       error
	)

	pNetBlock.IsSyncDataBackendsInited.LockContext()
	if pNetBlock.IsSyncDataBackendsInited.Load() == solodbapitypes.MetaDataStateInited {
		goto PREPARE_DONE
	}

	// fanout
	pNetBlock.SyncDataBackends.Reset()
	for i, _ := range backends.Slice() {
		pNetBlock.SyncDataBackends.Append(backends.Arr[i], 0)
	}
	pNetBlock.IsSyncDataBackendsInited.Store(solodbapitypes.MetaDataStateInited)

PREPARE_DONE:
	pNetBlock.IsSyncDataBackendsInited.UnlockContext()
	return err
}

func (p *Solomq) PrepareNetBlockSyncDataBackends(uNetBlock solofsapitypes.NetBlockUintptr,
	syncDataBackends snettypes.PeerGroup) error {
	return p.doPrepareNetBlockSyncDataBackendsWithFanout(uNetBlock, syncDataBackends)
}

package broker

import (
	"soloos/common/sdbapitypes"
	"soloos/common/sdfsapitypes"
	"soloos/common/snettypes"
)

func (p *Broker) doPrepareNetBlockSyncDataBackendsWithFanout(uNetBlock sdfsapitypes.NetBlockUintptr,
	backends snettypes.PeerGroup,
) error {
	var (
		pNetBlock = uNetBlock.Ptr()
		err       error
	)

	pNetBlock.IsSyncDataBackendsInited.LockContext()
	if pNetBlock.IsSyncDataBackendsInited.Load() == sdbapitypes.MetaDataStateInited {
		goto PREPARE_DONE
	}

	// fanout
	pNetBlock.SyncDataBackends.Reset()
	for i, _ := range backends.Slice() {
		pNetBlock.SyncDataBackends.Append(backends.Arr[i], 0)
	}
	pNetBlock.IsSyncDataBackendsInited.Store(sdbapitypes.MetaDataStateInited)

PREPARE_DONE:
	pNetBlock.IsSyncDataBackendsInited.UnlockContext()
	return err
}

func (p *Broker) PrepareNetBlockSyncDataBackends(uNetBlock sdfsapitypes.NetBlockUintptr,
	syncDataBackends snettypes.PeerGroup) error {
	return p.doPrepareNetBlockSyncDataBackendsWithFanout(uNetBlock, syncDataBackends)
}

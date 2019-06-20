package agent

import (
	"io"
	"soloos/common/sdfsapitypes"
)

func (p *TopicDriver) PReadMemBlockWithDisk(uNetINode sdfsapitypes.NetINodeUintptr,
	uNetBlock sdfsapitypes.NetBlockUintptr, netBlockIndex int32,
	uMemBlock sdfsapitypes.MemBlockUintptr, memBlockIndex int32,
	offset uint64, length int) (int, error) {
	return 0, io.EOF
}

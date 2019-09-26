package solomq

import (
	"io"
	"soloos/common/solofsapitypes"
)

func (p *TopicDriver) PReadMemBlockWithDisk(uNetINode solofsapitypes.NetINodeUintptr,
	uNetBlock solofsapitypes.NetBlockUintptr, netBlockIndex int32,
	uMemBlock solofsapitypes.MemBlockUintptr, memBlockIndex int32,
	offset uint64, length int) (int, error) {
	return 0, io.EOF
}

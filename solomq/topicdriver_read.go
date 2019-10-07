package solomq

import (
	"io"
	"soloos/common/solofstypes"
)

func (p *TopicDriver) PReadMemBlockWithDisk(uNetINode solofstypes.NetINodeUintptr,
	uNetBlock solofstypes.NetBlockUintptr, netBlockIndex int32,
	uMemBlock solofstypes.MemBlockUintptr, memBlockIndex int32,
	offset uint64, length int) (int, error) {
	return 0, io.EOF
}

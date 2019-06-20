package types

import (
	"soloos/sdbone/offheap"
	"sync"
	"unsafe"
)

const (
	UploadMqBlockJobStructSize = unsafe.Sizeof(UploadMqBlockJob{})
)

type UploadMqBlockJobUintptr uintptr

func (u UploadMqBlockJobUintptr) Ptr() *UploadMqBlockJob {
	return (*UploadMqBlockJob)(unsafe.Pointer(u))
}

type UploadMqBlockJob struct {
	MetaDataStateMutex     sync.Mutex
	MetaDataState          MetaDataState
	SyncDataSig            sync.WaitGroup
	UNetINode              NetINodeUintptr
	UNetBlock              NetBlockUintptr
	UMqBlock               MqBlockUintptr
	MqBlockIndex           int32
	UploadMaskWaitingIndex int
	UploadMask             [2]offheap.ChunkMask
	UploadMaskWaiting      offheap.ChunkMaskUintptr
	UploadMaskProcessing   offheap.ChunkMaskUintptr
}

func (p *UploadMqBlockJob) Reset() {
	p.MetaDataState.Store(MetaDataStateUninited)
}

func (p *UploadMqBlockJob) UploadMaskSwap() {
	if p.UploadMaskWaitingIndex == 0 {
		p.UploadMaskWaiting = offheap.ChunkMaskUintptr(unsafe.Pointer(&p.UploadMask[1]))
		p.UploadMaskProcessing = offheap.ChunkMaskUintptr(unsafe.Pointer(&p.UploadMask[0]))
		p.UploadMaskWaitingIndex = 1
	} else {
		p.UploadMaskWaiting = offheap.ChunkMaskUintptr(unsafe.Pointer(&p.UploadMask[0]))
		p.UploadMaskProcessing = offheap.ChunkMaskUintptr(unsafe.Pointer(&p.UploadMask[1]))
		p.UploadMaskWaitingIndex = 0
	}
}

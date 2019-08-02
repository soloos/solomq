package swaltypes

import (
	"reflect"
	"soloos/common/log"
	"soloos/common/snettypes"
	"soloos/sdbone/offheap"
	"sync"
	"unsafe"
)

const (
	MqBlockStructSize = unsafe.Sizeof(MqBlock{})
)

type MqBlockUintptr uintptr

func (u MqBlockUintptr) Ptr() *MqBlock {
	return (*MqBlock)(unsafe.Pointer(u))
}

type MqBlock struct {
	offheap.HKVTableObjectWithBytes12
	RebaseNetBlockMutex sync.Mutex
	Bytes               reflect.SliceHeader
	AvailMask           offheap.ChunkMask
	UploadJob           UploadMqBlockJob
}

func (p *MqBlock) Contains(offset, end int) bool {
	return p.AvailMask.Contains(offset, end)
}

func (p *MqBlock) PWriteWithConn(conn *snettypes.Connection, length int, offset int) (isSuccess bool) {
	_, isSuccess = p.AvailMask.MergeIncludeNeighbour(offset, offset+length)
	if isSuccess {
		var err error
		if offset+length > p.Bytes.Cap {
			length = p.Bytes.Cap - offset
		}
		bytes := (*(*[]byte)(unsafe.Pointer(&p.Bytes)))
		err = conn.ReadAll(bytes[offset : offset+length])
		if err != nil {
			log.Warn("PWriteWithConn error", err)
			isSuccess = false
		}
	}
	return
}

func (p *MqBlock) PWriteWithMem(data []byte, offset int) (isSuccess bool) {
	_, isSuccess = p.AvailMask.MergeIncludeNeighbour(offset, offset+len(data))
	if isSuccess {
		copy((*(*[]byte)(unsafe.Pointer(&p.Bytes)))[offset:], data)
	}
	return
}

func (p *MqBlock) PReadWithConn(conn *snettypes.Connection, length int, offset int) error {
	var err error
	err = conn.WriteAll((*(*[]byte)(unsafe.Pointer(&p.Bytes)))[offset : offset+length])
	if err != nil {
		return err
	}
	return nil
}

func (p *MqBlock) PReadWithMem(data []byte, offset int) {
	copy(data, (*(*[]byte)(unsafe.Pointer(&p.Bytes)))[offset:])
}

func (p *MqBlock) GetUploadMqBlockJobUintptr() UploadMqBlockJobUintptr {
	return UploadMqBlockJobUintptr(unsafe.Pointer(p)) + UploadMqBlockJobUintptr(unsafe.Offsetof(p.UploadJob))
}

func (p *MqBlock) BytesSlice() *[]byte {
	return (*[]byte)(unsafe.Pointer(&p.Bytes))
}

func (p *MqBlock) Reset() {
	p.AvailMask.Reset()
	p.UploadJob.Reset()
	p.HKVTableObjectWithBytes12.Reset()
}

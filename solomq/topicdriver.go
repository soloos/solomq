package solomq

import (
	"soloos/common/solodbtypes"
	"soloos/common/solomqtypes"
	"soloos/solodb/offheap"
	"sync"
)

type TopicDriver struct {
	solomq *Solomq

	topicsByID   offheap.LKVTableWithInt64
	topicsByName sync.Map

	defaultNetBlockCap int
	defaultMemBlockCap int
}

func (p *TopicDriver) Init(solomq *Solomq,
	defaultNetBlockCap int, defaultMemBlockCap int,
) error {
	var err error

	p.solomq = solomq

	err = p.topicsByID.Init("SolomqTopic",
		int(solomqtypes.TopicStructSize), -1, offheap.DefaultKVTableSharedCount,
		p.topicsByIDInvokeBeforeReleaseObjectFunc)
	if err != nil {
		return err
	}

	p.defaultNetBlockCap = defaultNetBlockCap
	p.defaultMemBlockCap = defaultMemBlockCap

	return nil
}

func (p *TopicDriver) topicsByIDInvokeBeforeReleaseObjectFunc(uObject uintptr) {
	var uTopic = solomqtypes.TopicUintptr(uObject)
	var pTopic = uTopic.Ptr()
	p.CleanTopicAssitCache(&pTopic.Meta)
	uTopic.Ptr().Reset()
}

func (p *TopicDriver) commitFsINodeInCache(uTopic solomqtypes.TopicUintptr) error {
	var pTopic = uTopic.Ptr()
	p.topicsByName.Store(pTopic.Meta.TopicName.Str(), uTopic)
	pTopic.IsDBMetaDataInited.Store(solodbtypes.MetaDataStateInited)
	return nil
}

func (p *TopicDriver) topicsByIDPrepareNewObjectFunc(uTopic solomqtypes.TopicUintptr,
	afterSetNewObj offheap.KVTableAfterSetNewObj) bool {
	var isNewObjectSetted bool
	if afterSetNewObj != nil {
		afterSetNewObj()
		uTopic.Ptr().Meta.TopicID = uTopic.Ptr().ID
		isNewObjectSetted = true
	} else {
		isNewObjectSetted = false
	}
	return isNewObjectSetted
}

func (p *TopicDriver) DeleteTopicCache(uTopic solomqtypes.TopicUintptr,
	parentID solomqtypes.TopicID, name string) {
	p.topicsByID.ForceDeleteAfterReleaseDone(offheap.LKVTableObjectUPtrWithInt64(uTopic))
}

func (p *TopicDriver) CleanTopicAssitCache(pTopicMeta *solomqtypes.TopicMeta) {
	p.topicsByName.Delete(pTopicMeta.TopicName.Str())
}

func (p *TopicDriver) GetTopicByName(topicName string) (solomqtypes.TopicUintptr, error) {
	var (
		topicMeta solomqtypes.TopicMeta
		err       error
	)

	err = p.FetchTopicByNameFromDB(topicName, &topicMeta)
	if err != nil {
		return 0, err
	}

	return p.GetTopicByID(topicMeta.TopicID)
}

func (p *TopicDriver) GetTopicByID(topicID solomqtypes.TopicID) (solomqtypes.TopicUintptr, error) {
	var (
		uObject           offheap.LKVTableObjectUPtrWithInt64
		uTopic            solomqtypes.TopicUintptr
		pTopic            *solomqtypes.Topic
		afterSetNewObj    offheap.KVTableAfterSetNewObj
		isNewObjectSetted bool
		err               error
	)
	uObject, afterSetNewObj = p.topicsByID.MustGetObject(topicID)
	p.topicsByIDPrepareNewObjectFunc(solomqtypes.TopicUintptr(uObject), afterSetNewObj)
	uTopic = solomqtypes.TopicUintptr(uObject)
	pTopic = uTopic.Ptr()
	if isNewObjectSetted || uTopic.Ptr().IsDBMetaDataInited.Load() == solodbtypes.MetaDataStateUninited {
		pTopic.IsDBMetaDataInited.LockContext()
		if pTopic.IsDBMetaDataInited.Load() == solodbtypes.MetaDataStateUninited {
			err = p.FetchTopicByIDFromDB(topicID, &pTopic.Meta)
			if err != nil {
				p.ReleaseTopic(uTopic)
			} else {
				err = p.commitFsINodeInCache(uTopic)
			}
		}
		pTopic.IsDBMetaDataInited.UnlockContext()
	}

	if err != nil {
		return 0, err
	}

	return uTopic, nil
}

func (p *TopicDriver) MustGetTopic(topicName string,
	solomqMembers []solomqtypes.SolomqMember) (solomqtypes.TopicUintptr, error) {
	var (
		topicMeta solomqtypes.TopicMeta
	)

	topicMeta.TopicName.SetStr(topicName)
	topicMeta.SolomqMemberGroup.SetSolomqMembers(solomqMembers)

	p.InsertTopicInDB(&topicMeta)
	return p.GetTopicByName(topicName)
}

func (p *TopicDriver) ReleaseTopic(uTopic solomqtypes.TopicUintptr) {
	p.topicsByID.ReleaseObject(offheap.LKVTableObjectUPtrWithInt64(uTopic))
}

func (p *TopicDriver) computeTopicRole(uTopic solomqtypes.TopicUintptr) int {
	for _, backend := range uTopic.Ptr().Meta.SolomqMemberGroup.Slice() {
		if p.solomq.srpcPeer.ID == backend.PeerID {
			return backend.Role
		}
	}
	return solomqtypes.SolomqMemberRoleUnknown
}

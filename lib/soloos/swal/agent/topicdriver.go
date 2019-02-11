package agent

import (
	"soloos/common/sdbapitypes"
	"soloos/common/swalapitypes"
	"soloos/sdbone/offheap"
	"sync"
)

type TopicDriver struct {
	swalAgent *SWALAgent

	topicsByID   offheap.LKVTableWithInt64
	topicsByName sync.Map

	defaultNetBlockCap int
	defaultMemBlockCap int
}

func (p *TopicDriver) Init(swalAgent *SWALAgent,
	defaultNetBlockCap int, defaultMemBlockCap int,
) error {
	var err error

	p.swalAgent = swalAgent

	err = p.topicsByID.Init("SWALAgentTopic",
		int(swalapitypes.TopicStructSize), -1, offheap.DefaultKVTableSharedCount,
		p.topicsByIDInvokeBeforeReleaseObjectFunc)
	if err != nil {
		return err
	}

	p.defaultNetBlockCap = defaultNetBlockCap
	p.defaultMemBlockCap = defaultMemBlockCap

	return nil
}

func (p *TopicDriver) topicsByIDInvokeBeforeReleaseObjectFunc(uObject uintptr) {
	var uTopic = swalapitypes.TopicUintptr(uObject)
	var pTopic = uTopic.Ptr()
	p.CleanTopicAssitCache(&pTopic.Meta)
	uTopic.Ptr().Reset()
}

func (p *TopicDriver) commitFsINodeInCache(uTopic swalapitypes.TopicUintptr) error {
	var pTopic = uTopic.Ptr()
	p.topicsByName.Store(pTopic.Meta.TopicName.Str(), uTopic)
	pTopic.IsDBMetaDataInited.Store(sdbapitypes.MetaDataStateInited)
	return nil
}

func (p *TopicDriver) topicsByIDPrepareNewObjectFunc(uTopic swalapitypes.TopicUintptr,
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

func (p *TopicDriver) DeleteTopicCache(uTopic swalapitypes.TopicUintptr,
	parentID swalapitypes.TopicID, name string) {
	p.topicsByID.ForceDeleteAfterReleaseDone(offheap.LKVTableObjectUPtrWithInt64(uTopic))
}

func (p *TopicDriver) CleanTopicAssitCache(pTopicMeta *swalapitypes.TopicMeta) {
	p.topicsByName.Delete(pTopicMeta.TopicName.Str())
}

func (p *TopicDriver) GetTopicByName(topicName string) (swalapitypes.TopicUintptr, error) {
	var (
		topicMeta swalapitypes.TopicMeta
		err       error
	)

	err = p.FetchTopicByNameFromDB(topicName, &topicMeta)
	if err != nil {
		return 0, err
	}

	return p.GetTopicByID(topicMeta.TopicID)
}

func (p *TopicDriver) GetTopicByID(topicID swalapitypes.TopicID) (swalapitypes.TopicUintptr, error) {
	var (
		uObject           offheap.LKVTableObjectUPtrWithInt64
		uTopic            swalapitypes.TopicUintptr
		pTopic            *swalapitypes.Topic
		afterSetNewObj    offheap.KVTableAfterSetNewObj
		isNewObjectSetted bool
		err               error
	)
	uObject, afterSetNewObj = p.topicsByID.MustGetObject(topicID)
	p.topicsByIDPrepareNewObjectFunc(swalapitypes.TopicUintptr(uObject), afterSetNewObj)
	uTopic = swalapitypes.TopicUintptr(uObject)
	pTopic = uTopic.Ptr()
	if isNewObjectSetted || uTopic.Ptr().IsDBMetaDataInited.Load() == sdbapitypes.MetaDataStateUninited {
		pTopic.IsDBMetaDataInited.LockContext()
		if pTopic.IsDBMetaDataInited.Load() == sdbapitypes.MetaDataStateUninited {
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
	swalMembers []swalapitypes.SWALMember) (swalapitypes.TopicUintptr, error) {
	var (
		topicMeta swalapitypes.TopicMeta
	)

	topicMeta.TopicName.SetStr(topicName)
	topicMeta.SWALMemberGroup.SetSWALMembers(swalMembers)

	p.InsertTopicInDB(&topicMeta)
	return p.GetTopicByName(topicName)
}

func (p *TopicDriver) ReleaseTopic(uTopic swalapitypes.TopicUintptr) {
	p.topicsByID.ReleaseObject(offheap.LKVTableObjectUPtrWithInt64(uTopic))
}

func (p *TopicDriver) computeTopicRole(uTopic swalapitypes.TopicUintptr) int {
	for _, backend := range uTopic.Ptr().Meta.SWALMemberGroup.Slice() {
		if p.swalAgent.peerID == backend.PeerID {
			return backend.Role
		}
	}
	return swalapitypes.SWALMemberRoleUnknown
}

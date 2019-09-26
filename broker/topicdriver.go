package broker

import (
	"soloos/common/solodbapitypes"
	"soloos/common/solomqapitypes"
	"soloos/solodb/offheap"
	"sync"
)

type TopicDriver struct {
	broker *Broker

	topicsByID   offheap.LKVTableWithInt64
	topicsByName sync.Map

	defaultNetBlockCap int
	defaultMemBlockCap int
}

func (p *TopicDriver) Init(broker *Broker,
	defaultNetBlockCap int, defaultMemBlockCap int,
) error {
	var err error

	p.broker = broker

	err = p.topicsByID.Init("BrokerTopic",
		int(solomqapitypes.TopicStructSize), -1, offheap.DefaultKVTableSharedCount,
		p.topicsByIDInvokeBeforeReleaseObjectFunc)
	if err != nil {
		return err
	}

	p.defaultNetBlockCap = defaultNetBlockCap
	p.defaultMemBlockCap = defaultMemBlockCap

	return nil
}

func (p *TopicDriver) topicsByIDInvokeBeforeReleaseObjectFunc(uObject uintptr) {
	var uTopic = solomqapitypes.TopicUintptr(uObject)
	var pTopic = uTopic.Ptr()
	p.CleanTopicAssitCache(&pTopic.Meta)
	uTopic.Ptr().Reset()
}

func (p *TopicDriver) commitFsINodeInCache(uTopic solomqapitypes.TopicUintptr) error {
	var pTopic = uTopic.Ptr()
	p.topicsByName.Store(pTopic.Meta.TopicName.Str(), uTopic)
	pTopic.IsDBMetaDataInited.Store(solodbapitypes.MetaDataStateInited)
	return nil
}

func (p *TopicDriver) topicsByIDPrepareNewObjectFunc(uTopic solomqapitypes.TopicUintptr,
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

func (p *TopicDriver) DeleteTopicCache(uTopic solomqapitypes.TopicUintptr,
	parentID solomqapitypes.TopicID, name string) {
	p.topicsByID.ForceDeleteAfterReleaseDone(offheap.LKVTableObjectUPtrWithInt64(uTopic))
}

func (p *TopicDriver) CleanTopicAssitCache(pTopicMeta *solomqapitypes.TopicMeta) {
	p.topicsByName.Delete(pTopicMeta.TopicName.Str())
}

func (p *TopicDriver) GetTopicByName(topicName string) (solomqapitypes.TopicUintptr, error) {
	var (
		topicMeta solomqapitypes.TopicMeta
		err       error
	)

	err = p.FetchTopicByNameFromDB(topicName, &topicMeta)
	if err != nil {
		return 0, err
	}

	return p.GetTopicByID(topicMeta.TopicID)
}

func (p *TopicDriver) GetTopicByID(topicID solomqapitypes.TopicID) (solomqapitypes.TopicUintptr, error) {
	var (
		uObject           offheap.LKVTableObjectUPtrWithInt64
		uTopic            solomqapitypes.TopicUintptr
		pTopic            *solomqapitypes.Topic
		afterSetNewObj    offheap.KVTableAfterSetNewObj
		isNewObjectSetted bool
		err               error
	)
	uObject, afterSetNewObj = p.topicsByID.MustGetObject(topicID)
	p.topicsByIDPrepareNewObjectFunc(solomqapitypes.TopicUintptr(uObject), afterSetNewObj)
	uTopic = solomqapitypes.TopicUintptr(uObject)
	pTopic = uTopic.Ptr()
	if isNewObjectSetted || uTopic.Ptr().IsDBMetaDataInited.Load() == solodbapitypes.MetaDataStateUninited {
		pTopic.IsDBMetaDataInited.LockContext()
		if pTopic.IsDBMetaDataInited.Load() == solodbapitypes.MetaDataStateUninited {
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
	solomqMembers []solomqapitypes.SOLOMQMember) (solomqapitypes.TopicUintptr, error) {
	var (
		topicMeta solomqapitypes.TopicMeta
	)

	topicMeta.TopicName.SetStr(topicName)
	topicMeta.SOLOMQMemberGroup.SetSOLOMQMembers(solomqMembers)

	p.InsertTopicInDB(&topicMeta)
	return p.GetTopicByName(topicName)
}

func (p *TopicDriver) ReleaseTopic(uTopic solomqapitypes.TopicUintptr) {
	p.topicsByID.ReleaseObject(offheap.LKVTableObjectUPtrWithInt64(uTopic))
}

func (p *TopicDriver) computeTopicRole(uTopic solomqapitypes.TopicUintptr) int {
	for _, backend := range uTopic.Ptr().Meta.SOLOMQMemberGroup.Slice() {
		if p.broker.srpcPeer.ID == backend.PeerID {
			return backend.Role
		}
	}
	return solomqapitypes.SOLOMQMemberRoleUnknown
}

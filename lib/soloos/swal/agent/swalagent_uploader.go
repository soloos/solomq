package agent

import (
	"soloos/common/swalapitypes"
)

type swalAgentUploader struct {
	swalAgent             *SWALAgent
	uploadTopicMsgJobChan chan swalapitypes.UploadTopicMsgJobUintptr
}

func (p *swalAgentUploader) Init(swalAgent *SWALAgent) error {
	p.swalAgent = swalAgent
	return nil
}

func (p *swalAgentUploader) Serve() error {
	return p.cronUpload()
}

func (p *swalAgentUploader) Close() error {
	return nil
}

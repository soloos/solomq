package agent

import "soloos/common/sdfsapi"

func (p *SWALAgent) SetSDFSClient(sdfsClient sdfsapi.Client) error {
	p.sdfsClient = sdfsClient
	p.posixFS = p.sdfsClient.GetPosixFS()

	p.posixFS.NetBlockSetPReadMemBlockWithDisk(p.TopicDriver.PReadMemBlockWithDisk)
	p.posixFS.NetBlockSetUploadMemBlockWithDisk(p.TopicDriver.UploadMemBlockWithDisk)

	return nil
}

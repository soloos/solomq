package solomq

import "soloos/common/solofsapi"

func (p *Solomq) SetSolofsClient(solofsClient solofsapi.Client) error {
	p.solofsClient = solofsClient
	p.posixFS = p.solofsClient.GetPosixFS()

	p.posixFS.NetBlockSetPReadMemBlockWithDisk(p.TopicDriver.PReadMemBlockWithDisk)
	p.posixFS.NetBlockSetUploadMemBlockWithDisk(p.TopicDriver.UploadMemBlockWithDisk)

	return nil
}

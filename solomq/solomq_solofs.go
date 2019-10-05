package solomq

import "soloos/common/solofsapi"

func (p *Solomq) SetSolofsClient(solofsClient solofsapi.Client) error {
	p.solofsClient = solofsClient
	p.posixFs = p.solofsClient.GetPosixFs()

	p.posixFs.NetBlockSetPReadMemBlockWithDisk(p.TopicDriver.PReadMemBlockWithDisk)
	p.posixFs.NetBlockSetUploadMemBlockWithDisk(p.TopicDriver.UploadMemBlockWithDisk)

	return nil
}

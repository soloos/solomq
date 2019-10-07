package solomq

import "soloos/solofs/solofssdk"

func (p *Solomq) SetSolofsClient(solofsClient interface{}) error {
	p.solofsClient = solofsClient.(*solofssdk.Client)

	p.solofsClient.NetBlockSetPReadMemBlockWithDisk(p.TopicDriver.PReadMemBlockWithDisk)
	p.solofsClient.NetBlockSetUploadMemBlockWithDisk(p.TopicDriver.UploadMemBlockWithDisk)

	return nil
}

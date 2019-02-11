package agent

import "soloos/common/sdfsapi"

func (p *SWALAgent) RegisterSDFSClient(sdfsClient sdfsapi.Client) error {
	p.sdfsClient = sdfsClient
	p.posixFS = p.sdfsClient.GetPosixFS()
	return nil
}

package libswal

import "soloos/common/sdfsapi"

func (p *Client) SetSDFSClient(itSDFSClient interface{}) error {
	var err error
	p.sdfsClient = itSDFSClient.(sdfsapi.Client)

	err = p.clientDriver.SWALAgent.RegisterSDFSClient(p.sdfsClient)
	if err != nil {
		return err
	}

	return nil
}

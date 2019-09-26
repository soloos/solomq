package solomqsdk

import "soloos/common/solofsapi"

func (p *Client) SetSOLOFSClient(itSOLOFSClient interface{}) error {
	var err error
	p.solofsClient = itSOLOFSClient.(solofsapi.Client)

	err = p.clientDriver.broker.SetSOLOFSClient(p.solofsClient)
	if err != nil {
		return err
	}

	return nil
}

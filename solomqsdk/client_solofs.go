package solomqsdk

import "soloos/common/solofsapi"

func (p *Client) SetSolofsClient(itSolofsClient interface{}) error {
	var err error
	p.solofsClient = itSolofsClient.(solofsapi.Client)

	err = p.clientDriver.solomq.SetSolofsClient(p.solofsClient)
	if err != nil {
		return err
	}

	return nil
}

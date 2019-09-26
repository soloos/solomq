package solomqsdk

import (
	"soloos/common/soloosbase"
	"soloos/common/util"
	"testing"
)

func TestClient(t *testing.T) {
	var soloosEnv soloosbase.SoloosEnv
	util.AssertErrIsNil(soloosEnv.InitWithSNet(""))

	// var solomqPeerIDStr = "00"
	// var solomqServeAddr = "127.0.0.1:9211"
	// p.SoloosInstance = &soloosutils.So / oOSInstance

	// assert.NoError(t, clientDriver.Init(&soloosEnv,
	// &offheap.DefaultOffheapDriver, solomqServeAddr))

	// var client Client
	// assert.NoError(t, clientDriver.InitClient(&client))
}

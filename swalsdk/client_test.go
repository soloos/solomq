package swalsdk

import (
	"soloos/common/soloosbase"
	"soloos/common/util"
	"testing"
)

func TestClient(t *testing.T) {
	var soloOSEnv soloosbase.SoloOSEnv
	util.AssertErrIsNil(soloOSEnv.Init())

	// var swalAgentPeerIDStr = "00"
	// var swalAgentServeAddr = "127.0.0.1:9211"
	// p.SoloOSInstance = &soloosutils.So / oOSInstance

	// assert.NoError(t, clientDriver.Init(&soloOSEnv,
	// &offheap.DefaultOffheapDriver, swalAgentServeAddr))

	// var client Client
	// assert.NoError(t, clientDriver.InitClient(&client))
}

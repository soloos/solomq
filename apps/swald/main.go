package main

import (
	"os"
	"soloos/common/util"
	"soloos/swal/swald"
)

func main() {
	var (
		swaldIns swald.SWALD
		options  swald.Options
		err      error
	)

	optionsFile := os.Args[1]

	err = util.LoadOptionsFile(optionsFile, &options)
	util.AssertErrIsNil(err)

	util.AssertErrIsNil(swaldIns.Init(options))
	util.AssertErrIsNil(swaldIns.Serve())
}

package main

import (
	"os"
	"soloos/common/util"
	"soloos/solomq/solomqd"
)

func main() {
	var (
		solomqdIns solomqd.SolomqD
		options    solomqd.Options
		err        error
	)

	optionsFile := os.Args[1]

	err = util.LoadOptionsFile(optionsFile, &options)
	util.AssertErrIsNil(err)

	util.AssertErrIsNil(solomqdIns.Init(options))
	util.AssertErrIsNil(solomqdIns.Serve())
}

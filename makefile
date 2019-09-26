SOLOMQ_LDFLAGS += -X "soloos/solomq/version.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
SOLOMQ_LDFLAGS += -X "soloos/solomq/version.GitHash=$(shell git rev-parse HEAD)"
# SOLOMQ_PREFIX += GOTMPDIR=./go.build/tmp GOCACHE=./go.build/cache

all:solomqd

solomqd:
	$(SOLOMQ_PREFIX) go build -i -ldflags '$(SOLOMQ_LDFLAGS)' -o ./bin/solomqd ./apps/solomqd

include ./make/test
include ./make/bench

.PHONY:all solomqd test

SWAL_LDFLAGS += -X "soloos/swal/version.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
SWAL_LDFLAGS += -X "soloos/swal/version.GitHash=$(shell git rev-parse HEAD)"
# SWAL_PREFIX += GOTMPDIR=./go.build/tmp GOCACHE=./go.build/cache

SOLOOS_SWAL_PROTOS = $(shell find ./ -name '*.fbs'|grep -v vendor)
GENERATED_PROTOS = $(shell find ./ -name "*.fbs"|grep -v vendor| sed 's/\.fbs/\.fbs\.go/g')
SOURCES = $(shell find . -name '*.go') $(GENERATED_PROTOS)

%.fbs.go: $(SOLOOS_SWAL_PROTOS)
	flatc -o ./ -g $(SOLOOS_SWAL_PROTOS)

fbs: $(GENERATED_PROTOS)

all:swald

swald:
	$(SWAL_PREFIX) go build -i -ldflags '$(SWAL_LDFLAGS)' -o ./bin/swald swald

include ./make/test
include ./make/bench

.PHONY:all swald test

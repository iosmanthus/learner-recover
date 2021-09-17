VERSION = 2.0.0
GIT_COMMIT = $(shell git rev-list -1 HEAD)
GOMOD = github.com/iosmanthus/learner-recover

.PHONY: all clean build

all: build

build:
	go build -ldflags \
		"-X ${GOMOD}/version.GitCommit=$(GIT_COMMIT) \
		-X ${GOMOD}/version.Version=2.0.0"
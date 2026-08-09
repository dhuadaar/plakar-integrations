GO	= go
EXT	=

PLAKAR	= plakar
VERSION	= v1.1.0

GOOS   := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)
PTAR   := backblazes3_$(VERSION)_$(GOOS)_$(GOARCH).ptar

all: build

build:
	${GO} build -v -o b2StorageS3${EXT} ./plugin/storage

package: build
	rm -f $(PTAR)
	${PLAKAR} pkg create ./manifest.yaml ${VERSION}

uninstall:
	-${PLAKAR} pkg rm backblazes3

install: package
	${PLAKAR} pkg add ./$(PTAR)

reinstall: uninstall install

test:
	${GO} test -v ./...

check: test

run: test

clean:
	rm -f b2StorageS3${EXT} backblazes3_*.ptar

.PHONY: all build package uninstall install reinstall test check run clean

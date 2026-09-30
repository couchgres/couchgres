FAUXTON_DIST := internal/fauxton/dist/index.html
GOVULNCHECK_VERSION := v1.7.0
STATICCHECK_VERSION := v0.8.1
VERSION ?= dev
GIT_DESCRIBE = $$(git describe --tags --long --always --dirty 2>/dev/null || true)
GIT_COMMIT = $$(git rev-parse --verify HEAD 2>/dev/null || true)
VERSION_LDFLAGS = -X github.com/couchgres/couchgres/internal/buildinfo.Version=$(VERSION) \
	-X github.com/couchgres/couchgres/internal/buildinfo.GitDescribe=$(GIT_DESCRIBE) \
	-X github.com/couchgres/couchgres/internal/buildinfo.GitCommit=$(GIT_COMMIT)

.PHONY: all build fauxton test staticcheck vulncheck run clean-fauxton

all: build

fauxton: $(FAUXTON_DIST)

$(FAUXTON_DIST): internal/fauxton/update.sh
	./internal/fauxton/update.sh

build: fauxton
	go build -buildvcs=true -ldflags "$(VERSION_LDFLAGS)" -o couchgres ./cmd/couchgres
	go version -m couchgres

test: fauxton
	go test ./...

staticcheck:
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) ./...

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

run: fauxton
	go run -buildvcs=true -ldflags "$(VERSION_LDFLAGS)" ./cmd/couchgres couchgres.yaml

clean-fauxton:
	rm -rf internal/fauxton/dist

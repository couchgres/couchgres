FAUXTON_DIST := internal/fauxton/dist/index.html
GOVULNCHECK_VERSION := v1.7.0

.PHONY: all build fauxton test vulncheck run clean-fauxton

all: build

fauxton: $(FAUXTON_DIST)

$(FAUXTON_DIST): internal/fauxton/update.sh
	./internal/fauxton/update.sh

build: fauxton
	go build -o couchgres ./cmd/couchgres
	go version -m couchgres

test: fauxton
	go test ./...

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

run: fauxton
	go run ./cmd/couchgres couchgres.yaml

clean-fauxton:
	rm -rf internal/fauxton/dist

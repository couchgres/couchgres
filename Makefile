FAUXTON_DIST := internal/fauxton/dist/index.html
GOVULNCHECK_VERSION := v1.7.0
STATICCHECK_VERSION := v0.8.1

.PHONY: all build fauxton test staticcheck vulncheck run clean-fauxton

all: build

fauxton: $(FAUXTON_DIST)

$(FAUXTON_DIST): internal/fauxton/update.sh
	./internal/fauxton/update.sh

build: fauxton
	go build -o couchgres ./cmd/couchgres
	go version -m couchgres

test: fauxton
	go test ./...

staticcheck:
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) ./...

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

run: fauxton
	go run ./cmd/couchgres couchgres.yaml

clean-fauxton:
	rm -rf internal/fauxton/dist

FAUXTON_DIST := internal/fauxton/dist/index.html

.PHONY: all build fauxton test run clean-fauxton

all: build

fauxton: $(FAUXTON_DIST)

$(FAUXTON_DIST): internal/fauxton/update.sh
	./internal/fauxton/update.sh

build: fauxton
	go build -o couchgres ./cmd/couchgres

test: fauxton
	go test ./...

run: fauxton
	go run ./cmd/couchgres couchgres.yaml

clean-fauxton:
	rm -rf internal/fauxton/dist

GO        ?= go
PKGS      ?= ./...
FUZZTIME  ?= 30s
BENCHTIME ?= 1s

.PHONY: all test race bench fuzz lint vet staticcheck build clean

all: lint test

build:
	$(GO) build -o bin/ ./cmd/...

test:
	$(GO) test -count=1 $(PKGS)

race:
	$(GO) test -race -count=1 $(PKGS)

bench:
	$(GO) test -run='^$$' -bench=. -benchmem -benchtime=$(BENCHTIME) .

fuzz:
	$(GO) test -run='^$$' -fuzz=FuzzBlockDecode -fuzztime=$(FUZZTIME) ./internal/sstable
	$(GO) test -run='^$$' -fuzz=FuzzTableOpen -fuzztime=$(FUZZTIME) ./internal/sstable
	$(GO) test -run='^$$' -fuzz=FuzzWALReplay -fuzztime=$(FUZZTIME) ./internal/wal
	$(GO) test -run='^$$' -fuzz=FuzzBatchDecode -fuzztime=$(FUZZTIME) .

vet:
	$(GO) vet $(PKGS)

staticcheck:
	$(GO) run honnef.co/go/tools/cmd/staticcheck@latest $(PKGS)

lint: vet staticcheck

clean:
	rm -rf bin/

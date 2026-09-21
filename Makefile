.PHONY: tidy test test-race vet fmt fmt-check check build collector collector-validate smoke run clean

tidy:
	go mod tidy

test:
	go test -mod=readonly ./...

test-race:
	go test -mod=readonly -race -cover ./...

vet:
	go vet -mod=readonly ./...

fmt:
	gofmt -w *.go

fmt-check:
	@unformatted="$$(gofmt -l *.go)"; \
	if [ -n "$$unformatted" ]; then printf 'Run make fmt for these files:\n%s\n' "$$unformatted"; exit 1; fi

check: fmt-check vet test-race

build: collector

collector:
	./scripts/build-collector.sh

collector-validate:
	JEV_API_KEY=validation-placeholder ./_build/otelcol-jevtraces validate --config examples/otelcol/config.yaml

smoke:
	python3 scripts/smoke-traces.py

run:
	./_build/otelcol-jevtraces --config examples/otelcol/config.yaml

clean:
	rm -rf _build

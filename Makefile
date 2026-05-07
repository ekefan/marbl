# Version baked in at build time
VERSION=$(git describe --tags --always)
go build -ldflags="-s -w -X main.version=$(VERSION)" ./cmd/producer
# what does version mean?

# PGO profile would be added in a second  pass when we have a cpu profile
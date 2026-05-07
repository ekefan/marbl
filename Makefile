schema_init_tasks:
	migrate create -ext sql -dir storage/migrations -seq init_task
schema_add_comments:
	migrate create -ext sql -dir storage/migrations -seq add_comment
sqlc-gen:
	sqlc generate -f storage/sqlc.yaml
# # Version baked in at build time
# VERSION=$(git describe --tags --always)
# go build -ldflags="-s -w -X main.version=$(VERSION)" ./cmd/producer
# # what does version mean?

# # PGO profile would be added in a second  pass when we have a cpu profile






# UP ALTER TABLE tasks ADD COLUMN comment TEXT;
# DOWN ALTER TABLE tasks DROP COLUMN IF EXISTS comment;
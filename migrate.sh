#!/usr/bin/env bash
set -euo pipefail

DB_URL="${DB_URL:-postgres://marbl:marbl@localhost:5432/marbl?sslmode=disable}"
MIGRATIONS_PATH="${MIGRATIONS_PATH:-storage/migrations}"

CMD="${1:-up}"
VERSION="${2:-}"

echo "➡️ Running migration command: $CMD"

case "$CMD" in
  up)
    migrate -path "$MIGRATIONS_PATH" -database "$DB_URL" up
    ;;
  
  down)
    migrate -path "$MIGRATIONS_PATH" -database "$DB_URL" down
    ;;

  force)
    if [[ -z "$VERSION" ]]; then
      echo "❌ version required for force"
      exit 1
    fi
    migrate -path "$MIGRATIONS_PATH" -database "$DB_URL" force "$VERSION"
    ;;

  goto)
    if [[ -z "$VERSION" ]]; then
      echo "❌ version required for goto"
      exit 1
    fi
    migrate -path "$MIGRATIONS_PATH" -database "$DB_URL" goto "$VERSION"
    ;;

  version)
    migrate -path "$MIGRATIONS_PATH" -database "$DB_URL" version
    ;;
  list)
    echo "Available migrations:"
    for file in "$MIGRATIONS_PATH"/*.up.sql; do
        base=$(basename "$file")
        version=$(echo "$base" | cut -d '_' -f1)
        title=$(echo "$base" | sed -E 's/^[0-9]+_//' | sed 's/\.up\.sql$//')
        echo "$version - $title"
    done
    ;;
  *)
    echo "Usage:"
    echo "  migrate.sh up"
    echo "  migrate.sh down"
    echo "  migrate.sh goto <version>"
    echo "  migrate.sh force <version>"
    echo "  migrate.sh version"
    exit 1
    ;;
esac
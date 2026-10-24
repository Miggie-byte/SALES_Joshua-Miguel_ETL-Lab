docker compose down -v
docker compose up -d
docker compose ps
docker compose exec dwh-postgres psql -U dwh -d dwh
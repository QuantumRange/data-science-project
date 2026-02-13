# Data Science Project

Domains:
```shell
docker compose exec -T db \
  psql -U postgres -d postgres \
  -c "COPY (
        SELECT id, domain
        FROM public.domain
      ) TO STDOUT WITH (FORMAT csv, HEADER true)" \
| zstd -T0 -6 -o domain.csv.zst
```

copy urls
```shell
docker compose exec -T db \
  psql -U postgres -d postgres \
  -c "COPY (
        SELECT us.id, us.path
        FROM public.url_strings us
        JOIN public.urls u ON u.id = us.id
        WHERE u.queued IS FALSE
      ) TO STDOUT (FORMAT binary)" \
| zstd -6 -T0 -o url_strings_queued_false.copy.zst
```

Place files in data folder and
`zstd -d *.zst`
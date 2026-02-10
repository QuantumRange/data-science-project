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
        FROM public.urls u
        JOIN public.url_strings us ON us.id = u.id
        WHERE u.queued IS FALSE
      ) TO STDOUT WITH (FORMAT csv)" \
| zstd -T0 -6 -o used_urls.csv.zst
```

Place files in data folder and
`zstd -d *.zst`
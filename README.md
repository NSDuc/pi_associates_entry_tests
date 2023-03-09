

```shell
docker build -t pi_associates:syduc -f Dockerfile .
```

```shell
docker-compose -f docker-compose-1-2-4.yaml up
docker exec -it pi_associates_syduc bash
```

```shell
docker-compose -f docker-compose-1-2-4.yaml down --volumes
```

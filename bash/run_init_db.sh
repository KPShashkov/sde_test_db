#!/bin/bash
docker pull postgres
docker run --name test_pos -e POSTGRES_PASSWORD=sde_password012 -e POSTGRES_USER=test_sde -e POSTGRES_DB=demo -p 5432:5432 -d postgres
mkdir ~/init_db
docker cp  ~/init_db test_pos:/sql
sleep 5
docker exec test_pos psql -U test_sde -d demo -f /sql/init_db/demo.sql

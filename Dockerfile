FROM postgres:17

COPY db.sql /docker-entrypoint-initdb.d/init.sql

FROM mongo:7.0

FROM rabbitmq:latest
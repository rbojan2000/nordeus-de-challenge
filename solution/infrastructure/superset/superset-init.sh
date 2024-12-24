#!/bin/bash

# create Admin user, you can read these values from env or anywhere else possible
superset fab create-admin --username "$ADMIN_USERNAME" --firstname Superset --lastname Admin --email "$ADMIN_EMAIL" --password "$ADMIN_PASSWORD"

# Upgrading Superset metastore
superset db upgrade

# setup roles and permissions
superset superset init 

superset set_database_uri \
    postgresql+psycopg2://nordeus_user:nordeus@postgres:5432/nordeus_db

# Starting server
/bin/sh -c /usr/bin/run-server.sh
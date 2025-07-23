#!/bin/bash
echo "🔄 Import du dump dans la base de données..."
pg_restore -U postgres -d gracethd /docker-entrypoint-initdb.d/gracethd.sql

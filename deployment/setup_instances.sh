#!/bin/bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update -y
apt-get install -y mysql-server wget unzip

systemctl enable mysql
systemctl start mysql

cd /tmp
wget -O sakila-db.zip https://downloads.mysql.com/docs/sakila-db.zip
unzip -o sakila-db.zip

if ! mysql -e "USE sakila;" 2>/dev/null; then
  mysql < /tmp/sakila-db/sakila-schema.sql
  mysql < /tmp/sakila-db/sakila-data.sql
fi

MYSQL_USER="bench"
MYSQL_PASS="benchpass"

mysql -e "CREATE USER IF NOT EXISTS '${MYSQL_USER}'@'%' IDENTIFIED WITH mysql_native_password BY '${MYSQL_PASS}';"
mysql -e "GRANT ALL PRIVILEGES ON sakila.* TO '${MYSQL_USER}'@'%' ;"
mysql -e "FLUSH PRIVILEGES;"

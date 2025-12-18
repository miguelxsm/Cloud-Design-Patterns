def build_manager_and_workers(mysql_user="proxyuser", mysql_pass="proxypass") -> str:
  return f"""#!/bin/bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update -y
apt-get install -y mysql-server wget unzip

# Permitir conexiones remotas (ProxySQL está en otra instancia)
# Ubuntu suele usar /etc/mysql/mysql.conf.d/mysqld.cnf
if [ -f /etc/mysql/mysql.conf.d/mysqld.cnf ]; then
  # Si existe bind-address, lo sustituye; si no, lo añade bajo [mysqld]
  if grep -q '^bind-address' /etc/mysql/mysql.conf.d/mysqld.cnf; then
    sed -i 's/^bind-address.*/bind-address = 0.0.0.0/' /etc/mysql/mysql.conf.d/mysqld.cnf
  else
    sed -i '/^\[mysqld\]/a bind-address = 0.0.0.0' /etc/mysql/mysql.conf.d/mysqld.cnf
  fi
fi

systemctl enable mysql
systemctl restart mysql

cd /tmp
wget -O sakila-db.zip https://downloads.mysql.com/docs/sakila-db.zip
unzip -o sakila-db.zip

if ! mysql -e "USE sakila;" 2>/dev/null; then
  mysql < /tmp/sakila-db/sakila-schema.sql
  mysql < /tmp/sakila-db/sakila-data.sql
fi

MYSQL_USER="{mysql_user}"
MYSQL_PASS="{mysql_pass}"

mysql -e "CREATE USER IF NOT EXISTS '${{MYSQL_USER}}'@'%' IDENTIFIED WITH mysql_native_password BY '${{MYSQL_PASS}}';"
mysql -e "GRANT ALL PRIVILEGES ON sakila.* TO '${{MYSQL_USER}}'@'%' ;"
mysql -e "FLUSH PRIVILEGES;"
"""

def build_proxysql_user_data(
    manager_ip: str,
    worker_ips: list[str],
    mysql_user: str = "proxyuser",
    mysql_pass: str = "proxypass",
) -> str:
    workers_sql_values = ", ".join([f"(20,'{ip}',3306,200)" for ip in worker_ips])

    return f"""#!/bin/bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update -y
apt-get install -y curl gnupg lsb-release ca-certificates mysql-client netcat-openbsd

curl -fsSL https://repo.proxysql.com/ProxySQL/repo_pub_key | gpg --dearmor -o /usr/share/keyrings/proxysql.gpg
echo "deb [signed-by=/usr/share/keyrings/proxysql.gpg] https://repo.proxysql.com/ProxySQL/proxysql-2.7.x/$(lsb_release -cs)/ ./" \
  > /etc/apt/sources.list.d/proxysql.list

apt-get update -y
apt-get install -y proxysql

systemctl enable proxysql

# Forzar frontend MySQL a 3306 en el cnf (SIN regex con paréntesis)
sed -i 's/interfaces="0.0.0.0:6033"/interfaces="0.0.0.0:3306"/' /etc/proxysql.cnf || true
sed -i 's/interfaces="0.0.0.0:6033;\\/tmp\\/proxysql.sock"/interfaces="0.0.0.0:3306;\\/tmp\\/proxysql.sock"/' /etc/proxysql.cnf || true

# Arranque limpio: borrar DB interna para que no “recuerde” 6033
systemctl stop proxysql || true
rm -f /var/lib/proxysql/proxysql.db || true
systemctl start proxysql

# Espera a admin port
for i in $(seq 1 30); do
  nc -z 127.0.0.1 6032 && break
  sleep 1
done
if ! nc -z 127.0.0.1 6032; then
  echo "ERROR: proxysql did not open 6032"
  systemctl status proxysql --no-pager || true
  journalctl -u proxysql -n 300 --no-pager || true
  exit 1
fi

# Backends
mysql -u admin -padmin -h 127.0.0.1 -P 6032 -e "
DELETE FROM mysql_servers;
INSERT INTO mysql_servers(hostgroup_id,hostname,port,max_connections) VALUES
(10,'{manager_ip}',3306,200),
{workers_sql_values};
LOAD MYSQL SERVERS TO RUNTIME;
SAVE MYSQL SERVERS TO DISK;
"

# Usuario
mysql -u admin -padmin -h 127.0.0.1 -P 6032 -e "
DELETE FROM mysql_users;
INSERT INTO mysql_users(username,password,default_hostgroup) VALUES
('{mysql_user}','{mysql_pass}',10);
LOAD MYSQL USERS TO RUNTIME;
SAVE MYSQL USERS TO DISK;
"

# Reglas
mysql -u admin -padmin -h 127.0.0.1 -P 6032 -e "
DELETE FROM mysql_query_rules;
INSERT INTO mysql_query_rules(rule_id,active,match_pattern,destination_hostgroup,apply) VALUES
(1,1,'^SELECT.*FOR UPDATE',10,1),
(2,1,'^SELECT',20,1);
LOAD MYSQL QUERY RULES TO RUNTIME;
SAVE MYSQL QUERY RULES TO DISK;
"

ss -lntp | egrep '3306|6032|6033|proxysql' || true
"""

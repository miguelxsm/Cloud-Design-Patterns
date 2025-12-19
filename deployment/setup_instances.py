import textwrap


def _ensure_mysqld_option_block(option_lines: str) -> str:
    """
    Helper: returns a bash snippet that ensures each line in option_lines exists
    under [mysqld] in /etc/mysql/mysql.conf.d/mysqld.cnf (Ubuntu mysql-server).
    """
    # We keep it simple: if a key exists, replace; else append under [mysqld].
    # option_lines must be "key = value" lines (one per line).
    lines = [ln.strip() for ln in option_lines.strip().splitlines() if ln.strip()]
    bash = []
    bash.append('CNF="/etc/mysql/mysql.conf.d/mysqld.cnf"')
    bash.append('if [ -f "$CNF" ]; then')
    for ln in lines:
        key = ln.split("=", 1)[0].strip()
        # Replace if present, else add under [mysqld]
        bash.append(f'  if grep -qE "^{key}\\s*=" "$CNF"; then')
        bash.append(f'    sed -i "s|^{key}\\s*=.*|{ln}|" "$CNF"')
        bash.append("  else")
        bash.append(f'    sed -i "/^\\[mysqld\\]/a {ln}" "$CNF"')
        bash.append("  fi")
    bash.append("fi")
    return "\n".join(bash)


def build_manager_user_data(
    mysql_user: str,
    mysql_pass: str,
    server_id: int = 1,
    repl_user: str = "repl",
    repl_pass: str = "replpass",
) -> str:
    mysqld_opts = f"""
bind-address = 0.0.0.0
server-id = {server_id}
log_bin = /var/log/mysql/mysql-bin.log
binlog_do_db = sakila
gtid_mode = ON
enforce_gtid_consistency = ON
log_replica_updates = ON
"""
    ensure_opts = _ensure_mysqld_option_block(mysqld_opts)

    return f"""#!/bin/bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update -y
apt-get install -y mysql-server wget unzip

{ensure_opts}

systemctl enable mysql
systemctl restart mysql

# Esperar a MySQL
for i in $(seq 1 30); do
  mysqladmin ping --silent && break
  sleep 1
done

# Importar Sakila SOLO en el manager (source)
cd /tmp
wget -O sakila-db.zip https://downloads.mysql.com/docs/sakila-db.zip
unzip -o sakila-db.zip

if ! mysql -e "USE sakila;" 2>/dev/null; then
  mysql < /tmp/sakila-db/sakila-schema.sql
  mysql < /tmp/sakila-db/sakila-data.sql
fi

# Usuario app (para ProxySQL/cliente)
MYSQL_USER="{mysql_user}"
MYSQL_PASS="{mysql_pass}"

mysql -e "CREATE USER IF NOT EXISTS '${{MYSQL_USER}}'@'%' IDENTIFIED WITH mysql_native_password BY '${{MYSQL_PASS}}';"
mysql -e "GRANT ALL PRIVILEGES ON sakila.* TO '${{MYSQL_USER}}'@'%';"
mysql -e "FLUSH PRIVILEGES;"

# Usuario replicación (workers -> manager)
REPL_USER="{repl_user}"
REPL_PASS="{repl_pass}"

mysql -e "CREATE USER IF NOT EXISTS '${{REPL_USER}}'@'%' IDENTIFIED WITH mysql_native_password BY '${{REPL_PASS}}';"
mysql -e "GRANT REPLICATION SLAVE ON *.* TO '${{REPL_USER}}'@'%';"
mysql -e "FLUSH PRIVILEGES;"

# Diagnóstico básico
mysql -e "SHOW VARIABLES LIKE 'gtid_mode';"
mysql -e "SHOW VARIABLES LIKE 'log_bin';"
mysql -e "SHOW MASTER STATUS\\G" || true
"""


def build_workers_user_data(
    mysql_user: str,
    mysql_pass: str,
    manager_ip: str,
    server_id: int,
    repl_user: str = "repl",
    repl_pass: str = "replpass",
) -> str:
    mysqld_opts = f"""
bind-address = 0.0.0.0
server-id = {server_id}
gtid_mode = ON
enforce_gtid_consistency = ON
relay_log = /var/log/mysql/mysql-relay-bin.log
"""
    ensure_opts = _ensure_mysqld_option_block(mysqld_opts)

    return f"""#!/bin/bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update -y
apt-get install -y mysql-server

{ensure_opts}

systemctl enable mysql
systemctl restart mysql

# Esperar a MySQL
for i in $(seq 1 30); do
  mysqladmin ping --silent && break
  sleep 1
done

# Usuario app (para ProxySQL/cliente)
MYSQL_USER="{mysql_user}"
MYSQL_PASS="{mysql_pass}"

mysql -e "CREATE USER IF NOT EXISTS '${{MYSQL_USER}}'@'%' IDENTIFIED WITH mysql_native_password BY '${{MYSQL_PASS}}';"
mysql -e "GRANT ALL PRIVILEGES ON sakila.* TO '${{MYSQL_USER}}'@'%';"
mysql -e "FLUSH PRIVILEGES;"

# Configurar replicación con GTID auto-position (worker -> manager)
REPL_USER="{repl_user}"
REPL_PASS="{repl_pass}"
MANAGER_IP="{manager_ip}"

# MySQL 8: START REPLICA / CHANGE REPLICATION SOURCE TO

for i in $(seq 1 60); do
  nc -z "${{MANAGER_IP}}" 3306 && break
  sleep 2
done

for i in $(seq 1 60); do
  mysql -h "${{MANAGER_IP}}" -u repl -preplpass -e "SELECT 1" && break
  sleep 2
done

mysql -e "STOP REPLICA;" || true
mysql -e "RESET REPLICA ALL;" || true

mysql -e "CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='${{MANAGER_IP}}',
  SOURCE_USER='${{REPL_USER}}',
  SOURCE_PASSWORD='${{REPL_PASS}}',
  SOURCE_PORT=3306,
  SOURCE_AUTO_POSITION=1;"

mysql -e "START REPLICA;"

mysql -e "SET GLOBAL read_only = ON;"
mysql -e "SET GLOBAL super_read_only = ON;"


# Diagnóstico
mysql -e "SHOW REPLICA STATUS\\G" | egrep -i 'Replica_IO_Running|Replica_SQL_Running|Last_.*Error|Source_Host|Retrieved_Gtid_Set|Executed_Gtid_Set' || true
"""

def base_code_proxy(
    manager_ip: str,
    worker_ips: list[str],
    mysql_user: str = "proxyuser",
    mysql_pass: str = "proxypass",
) -> str:

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

sed -i 's/interfaces="0.0.0.0:6033"/interfaces="0.0.0.0:3306"/' /etc/proxysql.cnf || true
sed -i 's/interfaces="0.0.0.0:6033;\\/tmp\\/proxysql.sock"/interfaces="0.0.0.0:3306;\\/tmp\\/proxysql.sock"/' /etc/proxysql.cnf || true

systemctl stop proxysql || true
rm -f /var/lib/proxysql/proxysql.db || true
systemctl start proxysql

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

mysql -u admin -padmin -h 127.0.0.1 -P 6032 -e "
DELETE FROM mysql_servers;
INSERT INTO mysql_servers(hostgroup_id,hostname,port,max_connections) VALUES
(10,'{manager_ip}',3306,200);
LOAD MYSQL SERVERS TO RUNTIME;
SAVE MYSQL SERVERS TO DISK;
"

mysql -u admin -padmin -h 127.0.0.1 -P 6032 -e "
DELETE FROM mysql_users;
INSERT INTO mysql_users(username,password,default_hostgroup) VALUES
('{mysql_user}','{mysql_pass}',10);
LOAD MYSQL USERS TO RUNTIME;
SAVE MYSQL USERS TO DISK;
"
"""

def build_proxysql_user_data(
    manager_ip: str,
    worker_ips: list[str],
    mysql_user: str = "proxyuser",
    mysql_pass: str = "proxypass",
    strategy: str = "directhit",
    ping_period_sec: int = 5
) -> str:
    workers_sql_values = ", ".join([f"(20,'{ip}',3306,200)" for ip in worker_ips])

    rules_direct = r"""
    mysql -u admin -padmin -h 127.0.0.1 -P 6032 -e "
    DELETE FROM mysql_query_rules;
    INSERT INTO mysql_query_rules(rule_id,active,match_pattern,destination_hostgroup,apply) VALUES
    (1,1,'^SELECT',10,1);
    LOAD MYSQL QUERY RULES TO RUNTIME;
    SAVE MYSQL QUERY RULES TO DISK;
    "
    """

    rules_rw_split = r"""
    mysql -u admin -padmin -h 127.0.0.1 -P 6032 -e "
    DELETE FROM mysql_query_rules;
    INSERT INTO mysql_query_rules(rule_id,active,match_pattern,destination_hostgroup,apply) VALUES
    (1,1,'^SELECT.*FOR UPDATE',10,1),
    (2,1,'^SELECT',20,1);
    LOAD MYSQL QUERY RULES TO RUNTIME;
    SAVE MYSQL QUERY RULES TO DISK;
    "
    """

    servers_with_workers = f"""
    mysql -u admin -padmin -h 127.0.0.1 -P 6032 -e "
    DELETE FROM mysql_servers;
    INSERT INTO mysql_servers(hostgroup_id,hostname,port,max_connections) VALUES
    (10,'{manager_ip}',3306,200),
    {workers_sql_values};
    LOAD MYSQL SERVERS TO RUNTIME;
    SAVE MYSQL SERVERS TO DISK;
    "
    """

    controller = f"""#!/bin/bash
set -euo pipefail

ADMIN_HOST="127.0.0.1"
ADMIN_PORT="6032"
ADMIN_USER="admin"
ADMIN_PASS="admin"

PERIOD="{ping_period_sec}"
WORKERS=({" ".join(worker_ips)})

while true; do
  best_ip=""
  best_ms=999999

  for ip in "${{WORKERS[@]}}"; do
    out="$(ping -c 1 -W 1 "$ip" 2>/dev/null || true)"
    ms="$(echo "$out" | sed -n 's/.*time=\\([0-9.]*\\).*/\\1/p' | head -n1)"
    [ -z "$ms" ] && continue

    better="$(awk -v a="$ms" -v b="$best_ms" 'BEGIN {{ if (a+0 < b+0) print 1; else print 0 }}')"
    if [ "$better" = "1" ]; then
      best_ms="$ms"
      best_ip="$ip"
    fi
  done

  if [ -z "$best_ip" ]; then
    sleep "$PERIOD"
    continue
  fi

  sql="UPDATE mysql_servers SET weight = CASE hostname"
  for ip in "${{WORKERS[@]}}"; do
    if [ "$ip" = "$best_ip" ]; then
      sql="$sql WHEN '$ip' THEN 1000"
    else
      sql="$sql WHEN '$ip' THEN 1"
    fi
  done
  sql="$sql END WHERE hostgroup_id=20;"

  mysql -u "$ADMIN_USER" -p"$ADMIN_PASS" -h "$ADMIN_HOST" -P "$ADMIN_PORT" -e "$sql
LOAD MYSQL SERVERS TO RUNTIME;
"

  sleep "$PERIOD"
done
"""

    controller_install = textwrap.dedent(f"""\
cat > /usr/local/bin/proxysql_ping_controller.sh <<'EOC'
{controller}
EOC
chmod 0755 /usr/local/bin/proxysql_ping_controller.sh

cat > /etc/systemd/system/proxysql-ping-controller.service <<'EOS'
[Unit]
Description=ProxySQL Ping Controller (customized strategy)
After=network-online.target proxysql.service
Wants=network-online.target

[Service]
Type=simple
ExecStart=/bin/bash /usr/local/bin/proxysql_ping_controller.sh
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOS

systemctl daemon-reload
systemctl enable --now proxysql-ping-controller.service
""")

    if strategy == "direct_hit":
        extra = rules_direct
    elif strategy == "random":
        extra = servers_with_workers + rules_rw_split
    elif strategy == "customized":
        extra = servers_with_workers + rules_rw_split + controller_install
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    return base_code_proxy(manager_ip, worker_ips, mysql_user, mysql_pass) + extra

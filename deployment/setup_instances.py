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
    ping_period_sec: int = 1
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

# Parámetros de estabilidad
K_PINGS=3                 # mediana de k pings por ciclo
ALPHA="0.2"               # EMA alpha (0.1..0.3)
EPS_MS="0.1"              # epsilon en ms para evitar división rara
W_MIN=1
W_MAX=100
DELTA_MAX=10              # rate limit: cambio máximo de weight por ciclo

# Workers
WORKERS=({" ".join(worker_ips)})

# Estado EMA y pesos previos (associative arrays)
declare -A EMA
declare -A WPREV

# Inicialización: EMA grande y pesos en valor medio
INIT_EMA="100.0"
INIT_W=$(( (W_MIN + W_MAX) / 2 ))
for ip in "${{WORKERS[@]}}"; do
  EMA["$ip"]="$INIT_EMA"
  WPREV["$ip"]="$INIT_W"
done

median3() {{
  # median of 3 numbers (strings) using sort
  printf "%s\\n%s\\n%s\\n" "$1" "$2" "$3" | sort -n | sed -n '2p'
}}

measure_rtt_ms() {{
  # Devuelve rtt en ms como float (string). Si falla, devuelve vacío.
  local ip="$1"
  local vals=()

  local n=0
  while [ "$n" -lt "$K_PINGS" ]; do
    out="$(ping -c 1 -W 1 "$ip" 2>/dev/null || true)"
    ms="$(echo "$out" | sed -n 's/.*time=\\([0-9.]*\\).*/\\1/p' | head -n1)"
    if [ -n "$ms" ]; then
      vals+=("$ms")
      n=$((n+1))
    else
      # si un ping falla, no lo contamos; evitamos sesgo a 0
      n=$((n+1))
      vals+=("")
    fi
  done

  # Filtrar vacíos
  local clean=()
  for v in "${{vals[@]}}"; do
    [ -n "$v" ] && clean+=("$v")
  done

  if [ "${{#clean[@]}}" -eq 0 ]; then
    echo ""
    return 0
  fi

  if [ "${{#clean[@]}}" -eq 1 ]; then
    echo "${{clean[0]}}"
    return 0
  fi

  if [ "${{#clean[@]}}" -eq 2 ]; then
    # mediana de 2 -> media simple (suave y barata)
    awk -v a="${{clean[0]}}" -v b="${{clean[1]}}" 'BEGIN {{ printf "%.6f", (a+b)/2.0 }}'
    return 0
  fi

  # >=3: usamos mediana de 3 primeros (K_PINGS=3 recomendado)
  echo "$(median3 "${{clean[0]}}" "${{clean[1]}}" "${{clean[2]}}")"
}}

clip_int() {{
  # clip_int value min max
  local v="$1"; local lo="$2"; local hi="$3"
  if [ "$v" -lt "$lo" ]; then echo "$lo"; return; fi
  if [ "$v" -gt "$hi" ]; then echo "$hi"; return; fi
  echo "$v"
}}

while true; do
  # 1) medir RTT robusto y actualizar EMA
  declare -A RTT
  declare -A SCORE

  for ip in "${{WORKERS[@]}}"; do
    rtt="$(measure_rtt_ms "$ip")"
    if [ -z "$rtt" ]; then
      # Si no hay medida, no tocamos EMA; penalizamos con EMA actual
      rtt="${{EMA[$ip]}}"
    fi
    RTT["$ip"]="$rtt"

    prev="${{EMA[$ip]}}"
    new_ema="$(awk -v a="$ALPHA" -v r="$rtt" -v p="$prev" 'BEGIN {{ printf "%.6f", a*r + (1.0-a)*p }}')"
    EMA["$ip"]="$new_ema"
  done

  # 2) latencia->score: s_i = 1/(ema_i + eps)
  sum_scores="0.0"
  for ip in "${{WORKERS[@]}}"; do
    e="${{EMA[$ip]}}"
    s="$(awk -v e="$e" -v eps="$EPS_MS" 'BEGIN {{ printf "%.12f", 1.0/(e+eps) }}')"
    SCORE["$ip"]="$s"
    sum_scores="$(awk -v x="$sum_scores" -v y="$s" 'BEGIN {{ printf "%.12f", x+y }}')"
  done

  # Si sum_scores es 0 (muy improbable), saltar
  zero="$(awk -v s="$sum_scores" 'BEGIN {{ if (s<=0.0) print 1; else print 0 }}')"
  if [ "$zero" = "1" ]; then
    sleep "$PERIOD"
    continue
  fi

  # 3) score->peso: w_i* = wmin + (wmax-wmin)*(s_i/sum)
  declare -A WSTAR
  for ip in "${{WORKERS[@]}}"; do
    s="${{SCORE[$ip]}}"
    w="$(awk -v wmin="$W_MIN" -v wmax="$W_MAX" -v si="$s" -v sum="$sum_scores" '
      BEGIN {{
        v = wmin + (wmax-wmin)*(si/sum);
        # redondeo al entero más cercano
        if (v>=0) printf "%d", int(v+0.5); else printf "%d", int(v-0.5);
      }}')"
    # asegurar rango
    w="$(clip_int "$w" "$W_MIN" "$W_MAX")"
    WSTAR["$ip"]="$w"
  done

  # 4) rate limiting: w(t)=w(t-1)+clip(w*-wprev, -DELTA, +DELTA)
  declare -A WNEW
  for ip in "${{WORKERS[@]}}"; do
    prev="${{WPREV[$ip]}}"
    target="${{WSTAR[$ip]}}"
    delta=$(( target - prev ))
    if [ "$delta" -gt "$DELTA_MAX" ]; then delta="$DELTA_MAX"; fi
    if [ "$delta" -lt $(( -DELTA_MAX )) ]; then delta=$(( -DELTA_MAX )); fi
    new=$(( prev + delta ))
    new="$(clip_int "$new" "$W_MIN" "$W_MAX")"
    WNEW["$ip"]="$new"
  done

  # 5) aplicar UPDATE en ProxySQL solo si hay cambios
  changed=0
  for ip in "${{WORKERS[@]}}"; do
    if [ "${{WNEW[$ip]}}" -ne "${{WPREV[$ip]}}" ]; then
      changed=1
      break
    fi
  done

  if [ "$changed" = "1" ]; then
    sql="UPDATE mysql_servers SET weight = CASE hostname"
    for ip in "${{WORKERS[@]}}"; do
      sql="$sql WHEN '$ip' THEN ${{WNEW[$ip]}}"
    done
    sql="$sql END WHERE hostgroup_id=20;"

    mysql -u "$ADMIN_USER" -p"$ADMIN_PASS" -h "$ADMIN_HOST" -P "$ADMIN_PORT" -e "$sql
LOAD MYSQL SERVERS TO RUNTIME;
"

    for ip in "${{WORKERS[@]}}"; do
      WPREV["$ip"]="${{WNEW[$ip]}}"
    done
  fi

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

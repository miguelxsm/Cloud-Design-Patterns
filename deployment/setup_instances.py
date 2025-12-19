import textwrap
import base64


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

    if strategy == "directhit":
        extra = rules_direct
    elif strategy == "random":
        extra = servers_with_workers + rules_rw_split
    elif strategy == "customized":
        extra = servers_with_workers + rules_rw_split + controller_install
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    return base_code_proxy(manager_ip, worker_ips, mysql_user, mysql_pass) + extra

def def_server_code(api_key, proxy_host, proxy_port, db_user, db_password) -> str:
    template = r'''from __future__ import annotations

import os
import re
import time
import logging
from typing import Any, Optional, List, Dict, Tuple

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

import mysql.connector
from mysql.connector import pooling, Error as MySQLError


# ----------------------------
# Config (inlined from user-data)
# ----------------------------
API_KEY = {API_KEY!r}
PROXY_HOST = {PROXY_HOST!r}
PROXY_PORT = int({PROXY_PORT})
DB_USER = {DB_USER!r}
DB_PASSWORD = {DB_PASSWORD!r}

DB_NAME = None

MAX_ROWS = 500
MAX_RESULT_BYTES = 2_000_000

# Pool sizing
POOL_NAME = os.environ.get("POOL_NAME", "gatekeeper_pool")
POOL_SIZE = int(os.environ.get("POOL_SIZE", "10"))
POOL_RESET_SESSION = os.environ.get("POOL_RESET_SESSION", "true").lower() in ("1", "true", "yes")

# Policy: allowlist toggle
STRICT_ALLOWLIST = os.environ.get("STRICT_ALLOWLIST", "true").lower() in ("1", "true", "yes")

# Logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("gatekeeper")

DENY_REGEX = re.compile(
    r"""
    \b(
        DROP
        |TRUNCATE
        |ALTER
        |GRANT
        |REVOKE
        |CREATE\s+USER
        |CREATE\s+ROLE
        |SET\s+PASSWORD
        |SHUTDOWN
        |RELOAD
        |SUPER
        |FILE
        |LOAD\s+DATA
        |OUTFILE
        |INFILE
        |XA
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

ALLOW_TOPLEVEL = re.compile(r"^\s*(SELECT|INSERT|UPDATE|DELETE)\b", re.IGNORECASE)


def is_single_statement(sql: str) -> bool:
    s = sql.strip()
    if not s:
        return False
    if s.endswith(";"):
        s = s[:-1].rstrip()
    return ";" not in s


def normalize_sql(sql: str) -> str:
    return sql.strip()


def validate_query(sql: str) -> None:
    s = normalize_sql(sql)
    if not s:
        raise HTTPException(status_code=400, detail="empty query")
    if not is_single_statement(s):
        raise HTTPException(status_code=403, detail="query rejected: multiple statements")
    if DENY_REGEX.search(s):
        raise HTTPException(status_code=403, detail="query rejected: forbidden keyword")
    if STRICT_ALLOWLIST and not ALLOW_TOPLEVEL.match(s):
        raise HTTPException(status_code=403, detail="query rejected: statement not allowed")
    if len(s) > 50_000:
        raise HTTPException(status_code=413, detail="query too large")


def classify_query(sql: str) -> str:
    s = normalize_sql(sql).lstrip()
    m = re.match(r"^(SELECT|INSERT|UPDATE|DELETE)\b", s, flags=re.IGNORECASE)
    return m.group(1).lower() if m else "other"


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=50_000)


class SelectResponse(BaseModel):
    type: str = "select"
    columns: List[str]
    rows: List[List[Any]]
    row_count: int
    truncated: bool


class WriteResponse(BaseModel):
    type: str = "write"
    affected_rows: int


app = FastAPI(title="DB Gatekeeper", version="1.0")
_pool: Optional[pooling.MySQLConnectionPool] = None


def require_env() -> None:
    missing = []
    if not API_KEY: missing.append("API_KEY")
    if not PROXY_HOST: missing.append("PROXY_HOST")
    if not DB_USER: missing.append("DB_USER")
    if not DB_PASSWORD: missing.append("DB_PASSWORD")
    if missing:
        raise RuntimeError("Missing required values: " + ", ".join(missing))


def create_pool() -> pooling.MySQLConnectionPool:
    require_env()
    conn_kwargs = dict(
        host=PROXY_HOST,
        port=PROXY_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        autocommit=True,
        connection_timeout=5,
    )
    if DB_NAME:
        conn_kwargs["database"] = DB_NAME

    return mysql.connector.pooling.MySQLConnectionPool(
        pool_name=POOL_NAME,
        pool_size=POOL_SIZE,
        pool_reset_session=POOL_RESET_SESSION,
        **conn_kwargs,
    )


@app.on_event("startup")
def on_startup() -> None:
    global _pool
    _pool = create_pool()
    log.info("Gatekeeper started. proxy=%s:%s pool_size=%s", PROXY_HOST, PROXY_PORT, POOL_SIZE)


@app.get("/health")
def health() -> Dict[str, Any]:
    try:
        assert _pool is not None
        cnx = _pool.get_connection()
        try:
            cur = cnx.cursor()
            cur.execute("SELECT 1")
            _ = cur.fetchone()
        finally:
            cnx.close()
        return {{"ok": True}}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"unhealthy: {{e}}")


def auth_or_401(x_api_key: Optional[str]) -> None:
    if not x_api_key or x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")


def fetch_all_limited(cur) -> Tuple[List[str], List[List[Any]], bool]:
    columns = [desc[0] for desc in (cur.description or [])]
    rows: List[List[Any]] = []
    truncated = False
    approx_bytes = 0

    for _ in range(MAX_ROWS + 1):
        row = cur.fetchone()
        if row is None:
            break
        if len(rows) >= MAX_ROWS:
            truncated = True
            break

        row_list = list(row)
        rows.append(row_list)

        approx_bytes += sum(len(str(v)) for v in row_list)
        if approx_bytes > MAX_RESULT_BYTES:
            truncated = True
            break

    return columns, rows, truncated


@app.post("/query", response_model=Any)
def query_endpoint(
    req: QueryRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> Any:
    auth_or_401(x_api_key)

    sql = req.query
    validate_query(sql)

    qtype = classify_query(sql)
    start = time.time()

    try:
        assert _pool is not None
        cnx = _pool.get_connection()
        try:
            cur = cnx.cursor()
            cur.execute(sql)

            if qtype == "select":
                cols, rows, truncated = fetch_all_limited(cur)
                return SelectResponse(columns=cols, rows=rows, row_count=len(rows), truncated=truncated)

            affected = cur.rowcount if cur.rowcount is not None else 0
            return WriteResponse(affected_rows=int(affected))
        finally:
            cnx.close()

    except HTTPException:
        raise
    except MySQLError as e:
        msg = str(e)
        if "Can't connect" in msg or "Connection refused" in msg or "timeout" in msg.lower():
            raise HTTPException(status_code=502, detail="upstream database unavailable")
        raise HTTPException(status_code=400, detail=f"sql error: {{msg}}")
    except Exception:
        raise HTTPException(status_code=500, detail="internal error")
'''

    # Sustitución segura SOLO de los 5 parámetros (sin tocar llaves del código)
    # Usamos repr() para que queden strings Python válidos con comillas.
    return template.format(
        API_KEY=api_key,
        PROXY_HOST=proxy_host,
        PROXY_PORT=int(proxy_port),
        DB_USER=db_user,
        DB_PASSWORD=db_password,
    )

def build_gateway_user_data(
    server_code: str,
    listen_port: int = 80,
    app_dir: str = "/opt/gatekeeper",
    service_name: str = "gatekeeper",
) -> str:
    
    code_b64 = base64.b64encode(server_code.encode("utf-8")).decode("ascii")

    systemd_unit = f"""\
[Unit]
Description=FastAPI Gatekeeper
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory={app_dir}
ExecStart=/opt/gatekeeper/venv/bin/python -m uvicorn server:app --host 0.0.0.0 --port {listen_port}
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
"""


    user_data = f"""#!/bin/bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update -y
apt-get install -y python3 python3-venv python3-pip ca-certificates curl

mkdir -p /opt/gatekeeper
cd /opt/gatekeeper

# Crear virtualenv
python3 -m venv venv

# Activar venv
source venv/bin/activate

# Instalar dependencias dentro del venv
pip install --upgrade pip
pip install fastapi uvicorn mysql-connector-python


# Escribir app
mkdir -p {app_dir}
echo "{code_b64}" | base64 -d > {app_dir}/server.py

# Systemd service
cat > /etc/systemd/system/{service_name}.service <<'EOS'
{systemd_unit}
EOS

systemctl daemon-reload
systemctl enable --now {service_name}

# Smoke check local
sleep 2
curl -fsS http://127.0.0.1:{listen_port}/health || (journalctl -u {service_name} -n 200 --no-pager; exit 1)
"""

    return textwrap.dedent(user_data)

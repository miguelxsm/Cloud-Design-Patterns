# Cloud Design Patterns: MySQL Cluster with Proxy and Gatekeeper

This repository contains the implementation for the final assignment of the course LOG8415E - Advanced Concepts of Cloud Computing at École Polytechnique de Montréal (2025-2026). The project sets up a distributed MySQL cluster on AWS EC2 instances, incorporating the Proxy and Gatekeeper cloud design patterns for improved scalability, security, and load distribution. The student responsible for this implementation is Miguel Carrasco Guirao.

## Project Overview

The assignment requires deploying a MySQL cluster with:
- **1 Manager Node** (handles all WRITE operations like INSERT, UPDATE, DELETE).
- **2 Worker Nodes** (handle READ operations; data is replicated from the manager).
- **Proxy Pattern**: Implemented using ProxySQL as a load balancer and router. It separates READ/WRITE queries and supports three strategies:
  - **Direct Hit**: Forwards all requests directly to the manager node (no distribution logic).
  - **Random**: Randomly selects a worker for READ queries.
  - **Customized**: Measures ping times to workers and forwards READ queries to the one with the lowest response time.
- **Gatekeeper Pattern**: Consists of a Gateway (internet-facing) and Trusted Host (ProxySQL as internal). The Gateway validates requests (authentication, authorization, query safety) before forwarding to the Proxy. This minimizes attack surface by restricting direct access.
- **Replication**: MySQL replication is set up from the manager to workers (no direct writes to workers).
- **Benchmarking**: A custom Python script (`bench.py`) sends 1000 READ and 1000 WRITE requests in parallel to evaluate performance, generating TPS (transactions per second), latency metrics, and summaries.
- **Automation**: Infrastructure as Code (IaC) using Python and AWS SDK (Boto3) for creating/destroying EC2 instances, security groups, and configurations.
- **Database**: Uses the Sakila sample database for testing and benchmarking.
- **Instance Types**: 3 t2.micro for DB nodes, 1 t2.large for Proxy, 1 t2.large for Gateway.

The system ensures:
- All requests go through the Gateway → Proxy → DB nodes.
- Security: Restricted inbound rules (e.g., SSH only from operator IP, MySQL ports only from trusted sources).
- Validation: Standalone MySQL instances are benchmarked with sysbench before clustering.

This setup demonstrates cloud best practices for scalability (read/write separation), security (gatekeeping), and automation.

## Prerequisites

- **AWS Account**: With permissions to create EC2 instances, security groups, and VPC resources.
- **AWS CLI**: Configured with access keys (or use environment variables for Boto3).
- **Python 3.x**: For running the infrastructure scripts and benchmark tool.
- **Dependencies**:
  ```
  pip3 install requests boto3
  ```
- **SSH Key Pair**: An AWS key pair for instance access (configure in code).
- **MySQL Credentials**: Default user/password in code (`mysqluser`/`mysqlpassword`); update as needed.
- **API Key**: For Gateway authentication (default: `MY_API_KEY` in `bench.py`).

## Setup and Installation

1. **Clone the Repository**:
```
git clone https://github.com/miguelxsm/Cloud-Design-Patterns
cd Cloud-Design-Patterns
```

2. **Configure AWS Credentials**:
- Set up `~/.aws/credentials` or environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).

3. **Update Constants**:
- In `infrastructure/constants.py`: Set `MY_IP`, VPC ID (if not default), key pair name, AMI ID (Ubuntu), etc.
- In `bench.py`: Update `--gateway-url`, `--api-key`, and queries if needed.

4. **Deploy Infrastructure**:
- Run `main.py` with flags to create components step-by-step or all at once.
```
python main.py --sg --instances --proxy --gateway --strategy [directhit|random|customized]
```
- `--sg`: Create security groups (Gateway, Proxy, Main DB).
- `--instances`: Create 3 DB instances (manager + 2 workers) with MySQL + Sakila.
- `--proxy`: Create ProxySQL instance, configure routing based on `--strategy`.
- `--gateway`: Create Gateway instance, configure as Gatekeeper forwarding to Proxy.
- Without flags: Creates everything.
- The script saves instance IPs to `deployment/ips_info.json`.

## Benchmarking the Cluster

Use `bench.py` to send 1000 READ and 1000 WRITE requests in parallel:
```
python bench.py 
--gateway-url http://<GATEWAY_PUBLIC_IP> 
--api-key MY_API_KEY 
--strategy [directhit|random|customized] 
--reads 1000 --writes 1000 
--outdir ./benchmarking/[strategy]
```
- Outputs: `summary.csv`, `tps_timeseries.csv`, `latency_timeseries.csv`, `raw_requests.csv`.
- Example Results (from report): TPS ~800/sec, low latency; confirms correct routing.
- Run for each strategy and compare (e.g., customized may show lower latency due to ping-based selection).

## Cleanup

Destroy all resources to avoid costs:
```
python main.py --destroy
```
- This terminates instances and deletes security groups.

## Report and Documentation

- Full report: `report.pdf` (details benchmarking, Proxy/Gatekeeper implementation, results).
- Assignment spec: `LOG8415E - Final Assignment - Cloud Design Patterns.pdf`.
- Video Demo: Upload to Moodle (explains code, decisions, results).
- Key Sections in Report:
  - Benchmarking MySQL with sysbench.
  - Proxy Pattern (security groups, ProxySQL setup, routing strategies).
  - Gatekeeper Pattern (validation flow).
  - Automation with AWS SDK.
  - Results: TPS, latency, request handling.

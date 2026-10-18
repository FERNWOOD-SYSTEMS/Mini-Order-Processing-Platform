# Mini Order Processing Platform

A capstone project: a small, real, multi-service platform where customers place orders, a queue
holds the work, and a background worker processes each order. It is the same pattern behind
checkout flows, payment processing, and notification systems in production.

This repository contains **working application code only**: `orders-api/`, `order-worker/`,
`frontend/`, and `db/init.sql`, plus a `docker-compose.yml` that runs the local dependencies
(Postgres and a fake SQS). Everything below this point is what you will build: containers,
infrastructure, deployment, monitoring, CI/CD, and documentation.

This README is a step-by-step guide with real commands and code snippets, so you always know what
to type next. Where a snippet says "repeat this shape", you are expected to write the other copies
yourself.

**How this differs from the other capstone option:** that one runs on Kubernetes (EKS). This one runs
on **ECS Fargate** with managed AWS services (RDS, SQS, SNS, Secrets Manager) around it. Both cover
Docker, Terraform, GitHub Actions, CloudWatch, and documentation.

---

## Architecture

```
                         Internet
                            |
                  +---------v----------+
                  |  Application Load  |   public subnets
                  |      Balancer      |
                  +----+----------+----+
        /orders*, /health         everything else
                       |                |
              +--------v------+   +-----v--------+
              |  orders-api   |   |   frontend   |   ECS Fargate services
              |  (port 8001)  |   |   (nginx)    |   private subnets
              +---+------+----+   +--------------+
                  |      |
        save order|      | send message
                  |      |
        +---------v--+  +v-----------+  redrive after 3 failed receives
        |    RDS     |  | SQS: orders |--------------------------+
        | PostgreSQL |  +------+------+                          |
        +---------^--+         | long poll                +------v-------+
                  |            |                          | SQS: orders- |
                  |     +------v--------+                 |     dlq      |
                  +-----|  order-worker |                 +--------------+
              update    |  (no port)    |---> SNS notification (optional)
              status    +---------------+

        Secrets Manager holds the database password.  CloudWatch collects logs, metrics, alarms.
```

Three application services, one database, one queue, one dead-letter queue. The worker has no
public address at all, which is the point: background work does not need a front door.

---

## What's Provided

| Component | Path | What it does |
|---|---|---|
| Orders API | `orders-api/` | FastAPI service. `POST /orders` saves an order as `PENDING` and sends a message to SQS. `GET /orders` and `GET /orders/{id}` read orders back. `GET /health` is a liveness check for your load balancer. `GET /ready` also checks the database. |
| Order Worker | `order-worker/` | Long-polls SQS. For each message it marks the order `PROCESSING`, does the "work", then marks it `COMPLETED` and deletes the message. If processing fails it leaves the message alone so SQS retries it. Any item with `FAULTY` in its name always fails, so you can test retries and the dead-letter queue on demand. |
| Frontend | `frontend/` | A small static storefront: a form to place orders, and a live table (refreshes every 3 seconds) showing each order's status. |
| Database schema | `db/init.sql` | One `orders` table. Safe to run repeatedly. |
| Local dependencies | `docker-compose.yml`, `elasticmq.conf` | Postgres 16 and ElasticMQ (a fake SQS with a real dead-letter queue configured). |
| Helper scripts | `scripts/` | Place normal orders, place a deliberately bad order, check queue sizes. |

All components have been run end to end: normal orders move `PENDING` to `COMPLETED`, and a
`FAULTY` order is retried twice (`RETRYING`), then marked `FAILED` on the third attempt, after which
exactly one message appears in the dead-letter queue.

### Order lifecycle

```
PENDING --> PROCESSING --> COMPLETED
                |
                +--> RETRYING (receive 1 and 2 failed) --> PROCESSING --> ...
                |
                +--> FAILED   (receive 3 failed, message moves to the dead-letter queue)
```

**Unit tests:** `order-worker/test_worker.py` covers the worker's decision logic with no database or
AWS dependency:

```
cd order-worker
pip install -r requirements.txt pytest
python3 -m pytest test_worker.py -v
```

---

## Environment Variables

**Orders API**

| Variable | Purpose | Example / default |
|---|---|---|
| `DB_HOST` | Postgres hostname | `localhost` locally, the RDS address on AWS |
| `DB_PORT` | Postgres port | `5432` |
| `DB_NAME` | Database name | `orders` |
| `DB_USER` | Database user | `appuser` |
| `DB_PASSWORD` | Database password | (from Secrets Manager on AWS, never hardcoded) |
| `DB_SSLMODE` | SSL mode for the database connection | `prefer` (RDS may require SSL) |
| `SQS_QUEUE_URL` | URL of the `orders` queue | `http://localhost:9324/000000000000/orders` locally |
| `AWS_REGION` | Region for the SQS client | `us-east-1` |
| `AWS_ENDPOINT_URL` | Overrides the AWS endpoint. **Local only**, leave unset on AWS | `http://localhost:9324` |
| `PORT` | Port the API listens on | `8001` |

**Order Worker**: all the database and queue variables above (except `PORT`), plus:

| Variable | Purpose | Default |
|---|---|---|
| `MAX_RECEIVE_COUNT` | Receives before an order is marked `FAILED`. **Must equal `maxReceiveCount` in the queue's redrive policy.** | `3` |
| `PROCESSING_SECONDS` | How long the simulated work takes | `2` |
| `POISON_KEYWORD` | Items containing this word always fail | `FAULTY` |
| `WAIT_TIME_SECONDS` | SQS long-poll wait | `10` |
| `SNS_TOPIC_ARN` | If set, the worker publishes a message when an order completes | (unset) |

**Frontend:** set `window.API_BASE_URL` in `frontend/config.js`. For local development that is
`http://localhost:8001`. When the frontend and API share one load balancer, set it to an empty string
so the browser calls the same origin. A private service name will not work here: the browser needs an
address it can actually reach.

---

## PART 1: Run It Locally First (Before Any of This Touches AWS)

Prove the application works on your own machine before adding any cloud complexity. You need Python
3.11+, `pip`, Docker Desktop (or Docker Engine), and `curl`.

### 1.1: Start Postgres and the fake SQS

```
docker compose up -d
docker compose ps        # both services should be running (postgres should say healthy)
```

`db/init.sql` is applied to Postgres automatically the first time it starts.

### 1.2: Start each service, in separate terminals

**macOS / Linux / Git Bash / WSL**

```
# Terminal 1: Orders API (port 8001)
cd orders-api
pip install -r requirements.txt
DB_HOST=localhost DB_USER=appuser DB_PASSWORD=devpassword DB_NAME=orders \
SQS_QUEUE_URL=http://localhost:9324/000000000000/orders \
AWS_ENDPOINT_URL=http://localhost:9324 AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local \
python3 app.py

# Terminal 2: Order Worker (long-running, no port)
cd order-worker
pip install -r requirements.txt
DB_HOST=localhost DB_USER=appuser DB_PASSWORD=devpassword DB_NAME=orders \
SQS_QUEUE_URL=http://localhost:9324/000000000000/orders \
AWS_ENDPOINT_URL=http://localhost:9324 AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local \
python3 worker.py

# Terminal 3: Frontend (port 8080)
cd frontend
python3 -m http.server 8080
```

**Windows PowerShell** (the `VAR=value command` syntax above does not work in PowerShell)

```
# Terminal 1: Orders API
cd orders-api
pip install -r requirements.txt
$env:DB_HOST="localhost"; $env:DB_USER="appuser"; $env:DB_PASSWORD="devpassword"; $env:DB_NAME="orders"
$env:SQS_QUEUE_URL="http://localhost:9324/000000000000/orders"; $env:AWS_ENDPOINT_URL="http://localhost:9324"
$env:AWS_REGION="us-east-1"; $env:AWS_ACCESS_KEY_ID="local"; $env:AWS_SECRET_ACCESS_KEY="local"
python app.py
```

Do the same for the worker in a second terminal (`python worker.py`). The frontend is
`python -m http.server 8080` in `frontend/`.

### 1.3: Place orders and watch them process

Open http://localhost:8080, or use the scripts:

```
./scripts/place-orders.sh
```

(On Windows without Git Bash, use `curl.exe`, not `curl`, and put the JSON body in a file with
`-d "@order.json"` to avoid PowerShell quoting problems.)

Within a few seconds every order should move from `PENDING` to `COMPLETED`. Check the API:

```
curl -s http://localhost:8001/orders
```

### 1.4: Prove the dead-letter queue works

```
./scripts/place-bad-order.sh
```

Watch the order in the dashboard. It should go to `RETRYING`, then `RETRYING` again, then `FAILED`
after the third attempt (about 30 seconds in total, because the local visibility timeout is 10
seconds). Then confirm the message moved to the dead-letter queue:

```
./scripts/check-queues.sh
```

or open http://localhost:9325 and look at the `orders-dlq` queue.

**Checkpoint:** you must see (1) good orders reach `COMPLETED`, (2) the faulty order end as `FAILED`
with `attempts: 3`, and (3) exactly one message in `orders-dlq`. Do not move on until you have seen
all three. Read the worker's terminal output too: every line is a JSON object, and you will query
those exact fields in CloudWatch later.

---

## PART 2: Containerize Each Service with Docker

### 2.1: Dockerfile for `orders-api/Dockerfile`

```
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN adduser --disabled-password --gecos "" appuser
USER appuser

EXPOSE 8001
CMD ["python", "app.py"]
```

**Why each line matters:** `requirements.txt` is copied and installed *before* the rest of the code,
so Docker's layer cache skips reinstalling dependencies on every code change. The `appuser` step means
the container never runs as root.

### 2.2: Dockerfile for `order-worker/Dockerfile`

The worker has no web server and no port to expose. It is a long-running loop:

```
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN adduser --disabled-password --gecos "" appuser
USER appuser

CMD ["python", "worker.py"]
```

**Think about this:** the worker handles `SIGTERM` so it can finish its current message before ECS
stops it. That only works if Python receives the signal. The `CMD ["python", "worker.py"]` form
(JSON array, "exec form") makes Python the main process. What would happen with the shell form
`CMD python worker.py`? Find out, and write down what you learn.

### 2.3: Dockerfile for `frontend/Dockerfile`

The frontend is static files, so it just needs a web server:

```
FROM nginx:alpine

COPY index.html style.css app.js config.js /usr/share/nginx/html/

EXPOSE 80
```

`config.js` is deliberately separate from `app.js`. It is the one file that changes between
environments (local versus AWS).

### 2.4: Add a `.dockerignore` to each service folder

Keep `__pycache__/`, `.venv/`, `.env`, `.git`, and test files out of your images. Decide for yourself
what belongs in each one, and explain your choices in your documentation.

### 2.5: Build and smoke-test each image locally

```
docker build -t orders-api:local ./orders-api
docker build -t order-worker:local ./order-worker
docker build -t frontend:local ./frontend
```

**If you are on an Apple Silicon Mac (or any ARM machine):** Fargate runs x86_64 by default, so an
image built on your laptop will crash on AWS with `exec format error`. Build with
`--platform linux/amd64`, or choose ARM64 in your task definition. Pick one on purpose and document it.

---

## PART 3: Test the Full Stack Locally with Docker Compose

Add your three services to the existing `docker-compose.yml` (keep the `postgres` and `elasticmq`
services that are already there):

```
  orders-api:
    build: ./orders-api
    ports: ["8001:8001"]
    environment:
      DB_HOST: postgres
      DB_USER: appuser
      DB_PASSWORD: devpassword
      DB_NAME: orders
      SQS_QUEUE_URL: http://elasticmq:9324/000000000000/orders
      AWS_ENDPOINT_URL: http://elasticmq:9324
      AWS_REGION: us-east-1
      AWS_ACCESS_KEY_ID: local
      AWS_SECRET_ACCESS_KEY: local
    depends_on:
      postgres: { condition: service_healthy }
      elasticmq: { condition: service_started }

  order-worker:
    build: ./order-worker
    environment:
      # same as orders-api, minus the ports
    depends_on:
      postgres: { condition: service_healthy }
      elasticmq: { condition: service_started }

  frontend:
    build: ./frontend
    ports: ["8080:80"]
    depends_on: [orders-api]
```

Fill in the worker's environment yourself.

```
docker compose up --build
```

Notice that inside the Compose network, services reach each other by **service name** (`postgres`,
`elasticmq`), not `localhost`. Repeat the Part 1.3 and 1.4 tests against the containers, including
the dead-letter queue check. Stop the worker container mid-processing (`docker compose stop
order-worker`) and read its logs to see the graceful shutdown.

**Note on `docker-entrypoint-initdb.d`:** files mounted there run only the *first* time Postgres
starts with an empty data directory. If you change `init.sql`, run `docker compose down -v` so the
volume is recreated.

---

## PART 4: Terraform Foundation (State, Network, Registry)

Create a `terraform/` folder. Everything from here on is Terraform. Never create AWS resources by
clicking in the console, except the one-time state bucket below.

### 4.1: Create the remote state bucket (one time, by hand)

Terraform cannot store its state in a bucket that Terraform has not created yet, so this one bucket
is made with the CLI. Bucket names are global, so pick something unique:

```
aws s3api create-bucket --bucket YOUR-UNIQUE-NAME-tfstate --region us-east-1
aws s3api put-bucket-versioning --bucket YOUR-UNIQUE-NAME-tfstate \
  --versioning-configuration Status=Enabled
aws s3api put-public-access-block --bucket YOUR-UNIQUE-NAME-tfstate \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

Your state file will contain the generated database password later. This is why the bucket is
private, versioned, and encrypted. Do **not** delete this bucket when you run `terraform destroy`.

### 4.2: Provider, backend, variables

`terraform/main.tf`:

```
terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.95"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Backend blocks cannot use variables, so the bucket name is written out here.
  backend "s3" {
    bucket       = "YOUR-UNIQUE-NAME-tfstate"
    key          = "order-platform/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true   # state locking without a DynamoDB table (Terraform 1.10+)
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project = var.project
    }
  }
}
```

`terraform/variables.tf`:

```
variable "aws_region" {
  default = "us-east-1"
}

variable "project" {
  default = "order-platform"
}

variable "alert_email" {
  description = "Where CloudWatch alarm emails go"
  type        = string
}

variable "github_repo" {
  description = "org/repo that is allowed to deploy, for example your-username/mini-order-platform"
  type        = string
}
```

Put real values in `terraform/terraform.tfvars` (it is git-ignored).

### 4.3: Networking

`terraform/vpc.tf`:

```
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.project}-vpc"
  cidr = "10.0.0.0/16"

  azs             = ["${var.aws_region}a", "${var.aws_region}b"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24"]

  enable_nat_gateway = true
  single_nat_gateway = true   # one NAT is enough for a capstone and costs half as much as two
}
```

**Cost warning:** the NAT Gateway bills every hour it exists, whether or not traffic flows. Your
private tasks need it to reach ECR, Secrets Manager, SQS, and CloudWatch. Look up VPC endpoints as
an alternative, and decide what is right for your budget. Document the decision either way.

### 4.4: Security groups

`terraform/security-groups.tf`. Three groups, each one only trusting the layer in front of it:

```
resource "aws_security_group" "alb" {
  name   = "${var.project}-alb"
  vpc_id = module.vpc.vpc_id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "ecs_tasks" {
  name   = "${var.project}-ecs-tasks"
  vpc_id = module.vpc.vpc_id

  # Only the load balancer may talk to the tasks, on the two container ports.
  ingress {
    from_port       = 8001
    to_port         = 8001
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  ingress {
    from_port       = 80
    to_port         = 80
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "rds" {
  name   = "${var.project}-rds"
  vpc_id = module.vpc.vpc_id

  # Only the ECS tasks may reach the database. Not the internet, not your laptop.
  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs_tasks.id]
  }
}
```

### 4.5: ECR repositories

`terraform/ecr.tf`:

```
locals {
  services = ["orders-api", "order-worker", "frontend"]
}

resource "aws_ecr_repository" "app" {
  for_each             = toset(local.services)
  name                 = each.value
  image_tag_mutability = "MUTABLE"
  force_delete         = true   # without this, terraform destroy fails on any repo that holds images

  image_scanning_configuration {
    scan_on_push = true
  }
}

output "ecr_repository_urls" {
  value = { for name, repo in aws_ecr_repository.app : name => repo.repository_url }
}
```

**Hint:** `for_each` over a list creates one resource per item without copy-pasting the block three
times. You will use this pattern again for log groups and services.

### 4.6: Run it

```
cd terraform
terraform init
terraform plan     # read it before applying, and explain to yourself what it will create
terraform apply
```

---

## PART 5: Push Your First Images to ECR (Manual Bootstrap)

An ECS service cannot start until its image exists. Push version `1.0.0` of each image by hand
once. CI/CD takes over after this. Shown for `orders-api`:

```
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com

docker build --platform linux/amd64 -t orders-api:1.0.0 ./orders-api
docker tag orders-api:1.0.0 <ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/orders-api:1.0.0
docker push <ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/orders-api:1.0.0
```

Repeat for `order-worker` and `frontend`. Confirm in the console (or with `aws ecr list-images
--repository-name orders-api`) that all three are there.

---

## PART 6: Terraform Data and Messaging (Queue, Secret, Database, Alerts)

### 6.1: The queue and its dead-letter queue

`terraform/sqs.tf`:

```
resource "aws_sqs_queue" "orders_dlq" {
  name                      = "orders-dlq"
  message_retention_seconds = 1209600   # 14 days, the maximum, so failed orders wait for a human
}

resource "aws_sqs_queue" "orders" {
  name                       = "orders"
  visibility_timeout_seconds = 30   # must be longer than the worker's processing time
  receive_wait_time_seconds  = 10   # long polling

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.orders_dlq.arn
    maxReceiveCount     = 3         # must match the worker's MAX_RECEIVE_COUNT
  })
}
```

### 6.2: The database password

`terraform/secrets.tf`:

```
resource "random_password" "db" {
  length  = 24
  special = false
}

resource "aws_secretsmanager_secret" "db" {
  name                    = "${var.project}/db"
  recovery_window_in_days = 0   # delete immediately on destroy, so you can rebuild with the same name
}

resource "aws_secretsmanager_secret_version" "db" {
  secret_id = aws_secretsmanager_secret.db.id
  secret_string = jsonencode({
    username = "appuser"
    password = random_password.db.result
  })
}
```

### 6.3: RDS PostgreSQL

`terraform/rds.tf`:

```
resource "aws_db_subnet_group" "main" {
  name       = "${var.project}-db"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_db_instance" "orders" {
  identifier     = "${var.project}-db"
  engine         = "postgres"
  engine_version = "16"
  instance_class = "db.t4g.micro"

  allocated_storage = 20
  db_name           = "orders"
  username          = "appuser"
  password          = random_password.db.result

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false
  multi_az               = false   # a deliberate cost decision for a capstone, write it in your docs

  skip_final_snapshot = true   # fine for a capstone, never for production
  apply_immediately   = true
}
```

RDS takes 5 to 10 minutes to create. That is normal.

### 6.4: Alerts topic (and optional order notifications)

`terraform/sns.tf`:

```
resource "aws_sns_topic" "alerts" {
  name = "${var.project}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
```

AWS emails you a confirmation link. Alarms will not reach you until you click it.

### 6.5: Create the database tables

Terraform created an empty database. `db/init.sql` has not been applied to it, and RDS sits in a
private subnet that your laptop cannot reach. Choose one approach, and document which you picked and
why:

- **A one-off ECS task** that runs `psql -f` with the SQL file (`aws ecs run-task`).
- **ECS Exec** into a running `orders-api` task, then run SQL from there.
- **Make the services create the tables on startup** if they do not exist.

The SQL is idempotent (`IF NOT EXISTS`), so running it twice is safe.

---

## PART 7: Terraform Compute (IAM, Load Balancer, ECS)

### 7.1: IAM roles

ECS uses two different roles, and mixing them up is a classic mistake:

- **Execution role:** used by *ECS itself* to pull your image, write logs, and fetch secrets before
  your code starts.
- **Task role:** used by *your code* while it runs, to call SQS and SNS.

`terraform/iam.tf`:

```
data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_execution" {
  name               = "${var.project}-ecs-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "ecs_execution_secrets" {
  name = "read-db-secret"
  role = aws_iam_role.ecs_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = [aws_secretsmanager_secret.db.arn]
    }]
  })
}

# Task role for the API: it may only send messages to the orders queue.
resource "aws_iam_role" "orders_api_task" {
  name               = "${var.project}-orders-api-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy" "orders_api_sqs" {
  name = "send-to-orders-queue"
  role = aws_iam_role.orders_api_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:SendMessage"]
      Resource = [aws_sqs_queue.orders.arn]
    }]
  })
}
```

**Your turn:** write `order_worker_task` with the *minimum* permissions the worker needs:
`sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes`, and `sqs:ChangeMessageVisibility`
on the orders queue, plus `sns:Publish` if you enable notifications. The frontend needs no task role
at all.

### 7.2: Cluster and logs

`terraform/ecs.tf`:

```
resource "aws_ecs_cluster" "main" {
  name = var.project

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_cloudwatch_log_group" "app" {
  for_each          = toset(local.services)
  name              = "/ecs/${var.project}/${each.value}"
  retention_in_days = 14
}
```

### 7.3: Task definition (shown for `orders-api`)

```
resource "aws_ecs_task_definition" "orders_api" {
  family                   = "orders-api"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.orders_api_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name      = "orders-api"
    image     = "${aws_ecr_repository.app["orders-api"].repository_url}:1.0.0"
    essential = true

    portMappings = [{ containerPort = 8001, protocol = "tcp" }]

    environment = [
      { name = "DB_HOST", value = aws_db_instance.orders.address },
      { name = "DB_PORT", value = "5432" },
      { name = "DB_NAME", value = "orders" },
      { name = "DB_USER", value = "appuser" },
      { name = "AWS_REGION", value = var.aws_region },
      { name = "SQS_QUEUE_URL", value = aws_sqs_queue.orders.url },
    ]

    # The password never appears in your code or your task definition. ECS fetches the
    # "password" key from the secret's JSON and injects it as an environment variable.
    secrets = [
      { name = "DB_PASSWORD", valueFrom = "${aws_secretsmanager_secret.db.arn}:password::" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.app["orders-api"].name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "ecs"
      }
    }
  }])
}
```

Write `order_worker` (same shape, no port mapping, plus `MAX_RECEIVE_COUNT`) and `frontend` (port 80,
no secrets, no database variables) yourself.

### 7.4: Load balancer

`terraform/alb.tf`. One ALB, two target groups, one path rule:

```
resource "aws_lb" "main" {
  name               = "${var.project}-alb"
  load_balancer_type = "application"
  subnets            = module.vpc.public_subnets
  security_groups    = [aws_security_group.alb.id]
}

resource "aws_lb_target_group" "api" {
  name        = "${var.project}-api"
  port        = 8001
  protocol    = "HTTP"
  target_type = "ip"   # required for Fargate
  vpc_id      = module.vpc.vpc_id

  health_check {
    path    = "/health"
    matcher = "200"
  }
}

resource "aws_lb_target_group" "frontend" {
  name        = "${var.project}-frontend"
  port        = 80
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = module.vpc.vpc_id

  health_check {
    path    = "/"
    matcher = "200"
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.main.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.frontend.arn
  }
}

resource "aws_lb_listener_rule" "api" {
  listener_arn = aws_lb_listener.http.arn
  priority     = 10

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }

  condition {
    path_pattern {
      values = ["/orders", "/orders/*", "/health"]
    }
  }
}

output "alb_dns_name" {
  value = aws_lb.main.dns_name
}
```

Because the browser reaches both the page and the API through this one address, set
`window.API_BASE_URL = ""` in `frontend/config.js` before you build the production frontend image.

### 7.5: Services

```
resource "aws_ecs_service" "orders_api" {
  name            = "orders-api"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.orders_api.arn
  desired_count   = 2
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = module.vpc.private_subnets
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "orders-api"
    container_port   = 8001
  }

  depends_on = [aws_lb_listener.http]

  lifecycle {
    # CI/CD registers new task definition revisions. Without this, the next terraform apply
    # would roll your service back to the 1.0.0 image.
    ignore_changes = [task_definition]
  }
}
```

Write the `order_worker` service (no `load_balancer` block, `desired_count = 1` to start) and the
`frontend` service yourself.

### 7.6: Apply and verify

```
terraform apply
aws ecs describe-services --cluster order-platform --services orders-api order-worker frontend \
  --query "services[].{name:serviceName,running:runningCount,desired:desiredCount}"
curl http://$(terraform output -raw alb_dns_name)/health
```

Then open the load balancer address in a browser and repeat the Part 1 checkpoint on AWS: place
orders, see them reach `COMPLETED`, place the `FAULTY` order, see it end `FAILED`, and confirm the
message is in the real `orders-dlq` queue. Take screenshots as you go. You need them for your
documentation.

---

## PART 8: CI/CD with GitHub Actions

### 8.1: OIDC trust (no stored AWS keys)

GitHub proves its identity to AWS with a short-lived token. You never store an access key in GitHub.

`terraform/github-oidc.tf`:

```
resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

resource "aws_iam_role" "github_actions" {
  name = "${var.project}-github-actions"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
        StringLike = {
          # Just "org/repo". No "https://" prefix, and the case must match exactly.
          "token.actions.githubusercontent.com:sub" = "repo:${var.github_repo}:*"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "github_actions" {
  name = "deploy-to-ecs"
  role = aws_iam_role.github_actions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload", "ecr:PutImage", "ecr:BatchGetImage"
        ]
        Resource = [for r in aws_ecr_repository.app : r.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["ecs:DescribeTaskDefinition", "ecs:RegisterTaskDefinition"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["ecs:UpdateService", "ecs:DescribeServices"]
        Resource = "*"
      },
      {
        # Registering a task definition requires permission to hand the two roles to ECS.
        Effect = "Allow"
        Action = ["iam:PassRole"]
        Resource = [
          aws_iam_role.ecs_execution.arn,
          aws_iam_role.orders_api_task.arn,
          # add your worker's task role here too
        ]
      }
    ]
  })
}

output "github_actions_role_arn" {
  value = aws_iam_role.github_actions.arn
}
```

Tighten the `ecs:*` resources to your own cluster and services as an improvement. Apply it, then
add these under **Settings, Secrets and variables, Actions**:

- `AWS_ROLE_ARN` (the `github_actions_role_arn` output)
- `AWS_REGION` (`us-east-1`)

### 8.2: The workflow

`.github/workflows/pipeline.yml`:

```
name: Build and Deploy

on:
  push:
    branches: [main]

permissions:
  id-token: write
  contents: read

env:
  CLUSTER: order-platform

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -r order-worker/requirements.txt pytest
      - run: cd order-worker && python -m pytest test_worker.py -v

  deploy:
    needs: test
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        service: [orders-api, order-worker, frontend]
    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_ARN }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Log in to Amazon ECR
        id: ecr-login
        uses: aws-actions/amazon-ecr-login@v2

      - name: Build and push ${{ matrix.service }}
        id: build
        env:
          IMAGE: ${{ steps.ecr-login.outputs.registry }}/${{ matrix.service }}:${{ github.sha }}
        run: |
          docker build -t $IMAGE ./${{ matrix.service }}
          docker push $IMAGE
          echo "image=$IMAGE" >> "$GITHUB_OUTPUT"

      - name: Download the current task definition
        run: |
          aws ecs describe-task-definition --task-definition ${{ matrix.service }} \
            --query taskDefinition > task-def.json

      - name: Put the new image into the task definition
        id: render
        uses: aws-actions/amazon-ecs-render-task-definition@v1
        with:
          task-definition: task-def.json
          container-name: ${{ matrix.service }}
          image: ${{ steps.build.outputs.image }}

      - name: Deploy ${{ matrix.service }}
        uses: aws-actions/amazon-ecs-deploy-task-definition@v1
        with:
          task-definition: ${{ steps.render.outputs.task-definition }}
          service: ${{ matrix.service }}
          cluster: ${{ env.CLUSTER }}
          wait-for-service-stability: true
```

**Hint:** the `matrix` block runs the deploy job once per service, in parallel, so one file handles
all three images. This works only because your ECR repository, ECS service, task definition family,
and container are all named identically. That naming convention is a design decision worth
documenting.

**Prove it works:** change something visible (a heading in `frontend/index.html`), push to `main`,
watch the run, and confirm the change appears at the load balancer address with no manual steps.

---

## PART 9: Monitoring with CloudWatch

### 9.1: What to set up, at minimum

- **Logs:** every service already writes to its log group (Part 7.2). Learn to find one order's whole
  story across the API and the worker using Logs Insights.
- **A dashboard** (`aws_cloudwatch_dashboard`) with, at least: ALB request count and 5xx errors, ECS
  CPU and memory per service, SQS queue depth, SQS age of the oldest message, and dead-letter queue
  size.
- **Alarms** that notify the SNS topic from Part 6.4:
  - messages visible in the dead-letter queue (any order that gave up)
  - the oldest message in `orders` is getting old (the worker is falling behind or stopped)
  - ALB target 5xx errors
  - ECS service CPU or memory above a threshold
  - RDS CPU, and RDS free storage space
- **A log metric filter** that counts the worker's `order_failed` events.

### 9.2: Example alarm: any message in the dead-letter queue

```
resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${var.project}-dlq-not-empty"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.orders_dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"   # SQS publishes nothing for an idle queue
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}
```

SQS metrics arrive with a delay of a few minutes, so be patient when you test it.

### 9.3: Example: turn the worker's JSON logs into a metric

Because the worker logs one JSON object per line, CloudWatch can filter on its fields directly:

```
resource "aws_cloudwatch_log_metric_filter" "order_failed" {
  name           = "order-failed"
  log_group_name = aws_cloudwatch_log_group.app["order-worker"].name
  pattern        = "{ $.event = \"order_failed\" }"

  metric_transformation {
    name      = "OrderFailed"
    namespace = "OrderPlatform"
    value     = "1"
  }
}
```

### 9.4: Logs Insights query to find everything about one order

```
fields @timestamp, service, event, receive_count, error
| filter order_id = 3
| sort @timestamp asc
```

### 9.5: Prove the alarms work

Place the `FAULTY` order on AWS and screenshot the alarm going from `OK` to `ALARM` and the email you
receive. An alarm you never saw fire is an alarm you cannot trust.

---

## Stretch Goals (Optional)

Not required, but each one adds real weight to your submission:

- Auto scale the worker service on queue depth (Application Auto Scaling with a target tracking or
  step policy on `ApproximateNumberOfMessagesVisible`).
- HTTPS on the load balancer using ACM and your own domain, with an HTTP-to-HTTPS redirect.
- Split the Terraform into reusable modules (`network`, `data`, `compute`).
- A manual approval step in the pipeline before deployment (GitHub Environments).
- A deployment circuit breaker with automatic rollback on the ECS services, and a bad commit you
  push on purpose to prove it works.
- A `terraform plan` check on pull requests.
- Turn on SNS notifications from the worker (`SNS_TOPIC_ARN`) and receive an email for each completed
  order.
- A replay procedure for the dead-letter queue (SQS redrive) written up as a runbook.

---

## Cost Control

Student credits disappear quietly. These are the things that bill while they sit idle:

- **NAT Gateway:** hourly charge plus data processing
- **Application Load Balancer:** hourly charge
- **RDS instance:** hourly charge while running
- **Fargate tasks:** per second for every running task

Do these on day one:

1. Create an AWS Budget with an email alert at a low amount.
2. Run `terraform destroy` whenever you stop working for the day. Your Terraform and pipeline should
   rebuild everything in minutes. If they cannot, treat that as a bug in your setup.
3. After `terraform destroy`, check the console for leftovers (NAT gateways, load balancers, and
   elastic IPs are the usual ones).

Do not destroy the state bucket from Part 4.1.

---

## Documentation Checklist

Your final submission needs, in this repository:

- [ ] This README, updated with anything you changed or decided differently
- [ ] An architecture diagram (the one at the top is a starting point; update it if your design differs)
- [ ] A deployment guide summarizing Parts 4 to 8 in your own words, including how someone else would
      rebuild everything from scratch
- [ ] All Terraform, Dockerfiles, `.dockerignore` files, the updated `docker-compose.yml`, and the
      GitHub Actions workflow, committed
- [ ] The decisions you made on the open questions (how you created the database tables, ARM or
      x86, NAT Gateway or VPC endpoints, one ALB or two, and why)
- [ ] Screenshots: the storefront on the load balancer address, a successful pipeline run, the
      CloudWatch dashboard, and an alarm firing and recovering
- [ ] Evidence that the dead-letter queue works **on AWS**, not only locally
- [ ] A troubleshooting log with at least 3 real problems you hit, what the symptom was, and how you
      fixed it
- [ ] A short cost note: what you ran, roughly what it cost, and what you did to keep it down
- [ ] Confirmation that `terraform destroy` was run before submission (unless still under live demo)

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Local: API returns 502 "could not queue order" | ElasticMQ is not running, or `SQS_QUEUE_URL` / `AWS_ENDPOINT_URL` is wrong. Run `docker compose ps`. |
| Local: orders stay `PENDING` forever | The worker is not running, or it is pointed at a different queue than the API. Read the worker's output. |
| Local: `relation "orders" does not exist` | The schema was never applied, or you changed `init.sql` without `docker compose down -v`. |
| Local: in Compose, `connection refused` to `localhost` | Inside a container `localhost` is the container itself. Use the service names `postgres` and `elasticmq`. |
| Local: PowerShell says `DB_HOST=localhost` is not recognized | PowerShell needs `$env:DB_HOST="localhost"` (Part 1.2). |
| Faulty order ends `FAILED` but the DLQ is empty | The redrive policy is missing, or its `maxReceiveCount` does not match the worker's `MAX_RECEIVE_COUNT`. |
| Task log shows `exec format error` | The image was built for ARM (Apple Silicon). Rebuild with `--platform linux/amd64`, or set `cpu_architecture = "ARM64"`. |
| `CannotPullContainerError` | The image tag does not exist in ECR, or private tasks have no route out (NAT Gateway or VPC endpoints). |
| `ResourceInitializationError: unable to pull secrets` | The execution role cannot read the secret, there is no route to Secrets Manager, or the `valueFrom` key syntax is wrong. |
| ALB returns 502 or 503, target group shows unhealthy | Wrong container port, the health check path does not return 200, or the task security group does not allow the ALB's security group. |
| API logs `could not connect to server: Connection timed out` | The RDS security group does not allow the task security group, or `DB_HOST` is wrong. |
| API logs `no pg_hba.conf entry ... no encryption` | RDS requires SSL. Set `DB_SSLMODE=require` (or leave `prefer`). |
| API returns 502 "could not queue order" on AWS | The API's task role lacks `sqs:SendMessage`, or `SQS_QUEUE_URL` points at the wrong queue. |
| Worker logs `AccessDenied` on `ReceiveMessage` | Check the worker's **task role** (not the execution role) and its resource ARN. |
| Frontend loads but the table says it cannot reach the API | `config.js` still points at `localhost`, or the ALB path rule does not include `/orders*`. |
| `terraform apply` says the secret is scheduled for deletion | Add `recovery_window_in_days = 0` to the secret, or wait out the recovery window. |
| `terraform destroy` fails deleting an ECR repository | Add `force_delete = true` to the repository (Part 4.5). |
| `terraform apply` fails with `EntityAlreadyExists` on the OIDC provider | An earlier attempt (or another project) already created it. `terraform import` it, or reference it with a `data` source. |
| CI: `Not authorized to perform sts:AssumeRoleWithWebIdentity` | The trust policy `sub` does not match your repo exactly (case matters, no `https://`), or the workflow lacks `id-token: write`. |
| CI: `not authorized to perform: iam:PassRole` | Add every execution and task role to the PassRole statement (Part 8.1). |
| Deploys succeed but `terraform apply` later reverts the image | Missing `ignore_changes = [task_definition]` on the service (Part 7.5). |

Read your own logs before asking anyone for help. Almost every row above is visible in CloudWatch
Logs, in the ECS service's **Events** tab, or in the target group's health status.

If you get stuck, bring your questions to the group chat. Include what you ran, the exact error, and
what you already checked.

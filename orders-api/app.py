"""
Orders API
----------
Receives new orders, saves them in Postgres as PENDING, then puts a message
on an SQS queue so the order-worker can process them in the background.

Endpoints
  GET  /health        liveness check (no dependencies). Use this for the ALB health check.
  GET  /ready         readiness check (database reachable + queue configured)
  POST /orders        create an order           -> 201
  GET  /orders        latest orders (?limit=50)
  GET  /orders/{id}   one order                 -> 404 if it does not exist
"""
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone

import boto3
import psycopg2
import uvicorn
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field

SERVICE = "orders-api"


def log(event, level="INFO", **fields):
    """One JSON object per line. CloudWatch Logs Insights can query these fields directly."""
    print(
        json.dumps(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "service": SERVICE,
                "level": level,
                "event": event,
                **fields,
            }
        ),
        flush=True,
    )


DB_CONFIG = dict(
    host=os.environ.get("DB_HOST", "localhost"),
    port=int(os.environ.get("DB_PORT", "5432")),
    dbname=os.environ.get("DB_NAME", "orders"),
    user=os.environ.get("DB_USER", "appuser"),
    password=os.environ.get("DB_PASSWORD", ""),
    sslmode=os.environ.get("DB_SSLMODE", "prefer"),
    connect_timeout=5,
)

SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
# Only set this when talking to a local fake SQS (ElasticMQ). Leave it unset on AWS.
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL") or None

app = FastAPI(title="Orders API")

# The browser calls this API directly when running locally (different port = different origin).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_sqs = None


def sqs_client():
    global _sqs
    if _sqs is None:
        _sqs = boto3.client("sqs", region_name=AWS_REGION, endpoint_url=AWS_ENDPOINT_URL)
    return _sqs


@contextmanager
def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


class OrderIn(BaseModel):
    customer_name: str = Field(min_length=1, max_length=100)
    item: str = Field(min_length=1, max_length=100)
    quantity: int = Field(ge=1, le=100)


@app.exception_handler(psycopg2.OperationalError)
def database_unavailable(request, exc):
    log("database_unavailable", level="ERROR", error=str(exc).strip())
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready():
    if not SQS_QUEUE_URL:
        raise HTTPException(status_code=503, detail="SQS_QUEUE_URL is not configured")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
    return {"status": "ready"}


@app.post("/orders", status_code=201)
def create_order(order: OrderIn):
    if not SQS_QUEUE_URL:
        raise HTTPException(status_code=500, detail="SQS_QUEUE_URL is not configured")

    # 1. Save the order. The 'with' block commits before we touch the queue.
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO orders (customer_name, item, quantity)
            VALUES (%s, %s, %s)
            RETURNING *
            """,
            (order.customer_name, order.item, order.quantity),
        )
        row = cur.fetchone()

    # 2. Hand it to the worker.
    try:
        sqs_client().send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({"order_id": row["id"]}),
        )
    except (BotoCoreError, ClientError) as exc:
        reason = f"could not queue order: {exc}"
        log("queue_send_failed", level="ERROR", order_id=row["id"], error=str(exc))
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE orders SET status='FAILED', failure_reason=%s, updated_at=NOW() WHERE id=%s",
                (reason, row["id"]),
            )
        raise HTTPException(status_code=502, detail="could not queue order")

    log("order_created", order_id=row["id"], item=row["item"], quantity=row["quantity"])
    return row


@app.get("/orders")
def list_orders(limit: int = Query(50, ge=1, le=200)):
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM orders ORDER BY id DESC LIMIT %s", (limit,))
        return cur.fetchall()


@app.get("/orders/{order_id}")
def get_order(order_id: int):
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="order not found")
    return row


if __name__ == "__main__":
    log("starting", port=int(os.environ.get("PORT", "8001")), queue_configured=bool(SQS_QUEUE_URL))
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8001")))

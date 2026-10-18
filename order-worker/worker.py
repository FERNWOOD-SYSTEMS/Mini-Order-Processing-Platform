"""
Order Worker
------------
Long-polls the SQS queue. For each message it:
  1. loads the order from Postgres and marks it PROCESSING
  2. simulates fulfilling it (a short sleep)
  3. marks it COMPLETED, sends a notification, and deletes the message

If processing fails, the message is NOT deleted. SQS makes it visible again after the
visibility timeout and the worker tries again. After MAX_RECEIVE_COUNT receives, SQS moves the
message to the dead-letter queue (DLQ). The worker mirrors that rule in the database:
  receive 1 and 2 fail -> status RETRYING
  receive 3 fails      -> status FAILED  (and SQS moves the message to the DLQ)

Any order whose item contains the word FAULTY always fails. That is how you test the DLQ.

IMPORTANT: MAX_RECEIVE_COUNT here must equal maxReceiveCount in the queue's redrive policy.
"""
import json
import os
import signal
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import boto3
import psycopg2
from botocore.exceptions import BotoCoreError, ClientError
from psycopg2.extras import RealDictCursor

SERVICE = "order-worker"

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
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL") or None
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")  # optional

MAX_RECEIVE_COUNT = int(os.environ.get("MAX_RECEIVE_COUNT", "3"))
PROCESSING_SECONDS = float(os.environ.get("PROCESSING_SECONDS", "2"))
POISON_KEYWORD = os.environ.get("POISON_KEYWORD", "FAULTY").upper()
WAIT_TIME_SECONDS = int(os.environ.get("WAIT_TIME_SECONDS", "10"))

_running = True


def log(event, level="INFO", **fields):
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


class ProcessingError(Exception):
    """Raised when an order cannot be processed."""


# --------------------------------------------------------------------------
# Pure logic (no database, no AWS). This is what test_worker.py covers.
# --------------------------------------------------------------------------
def is_poison(item, keyword=None):
    """True if this item is one that must always fail."""
    keyword = (keyword or POISON_KEYWORD).upper()
    return keyword in item.upper()


def status_after_failure(receive_count, max_receives):
    """What status to record when processing fails on this delivery."""
    return "FAILED" if receive_count >= max_receives else "RETRYING"


def parse_order_id(body):
    """Extract the order id from a message body like '{"order_id": 7}'. Raises ValueError if bad."""
    try:
        data = json.loads(body)
        return int(data["order_id"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"malformed message body: {body!r}") from exc


# --------------------------------------------------------------------------
# Database and AWS helpers
# --------------------------------------------------------------------------
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


def fetch_order(order_id):
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
        return cur.fetchone()


def update_status(order_id, status, attempts, reason=None):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE orders
               SET status = %s, attempts = %s, failure_reason = %s, updated_at = NOW()
             WHERE id = %s
            """,
            (status, attempts, reason, order_id),
        )


def make_clients():
    sqs = boto3.client("sqs", region_name=AWS_REGION, endpoint_url=AWS_ENDPOINT_URL)
    sns = boto3.client("sns", region_name=AWS_REGION) if SNS_TOPIC_ARN else None
    return sqs, sns


def send_notification(sns, order):
    """Tell someone the order is done. Never allowed to fail the order itself."""
    log("notification_sent", order_id=order["id"], customer=order["customer_name"])
    if sns is None:
        return
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"Order {order['id']} completed",
            Message=f"{order['customer_name']} ordered {order['quantity']} x {order['item']}. Status: COMPLETED.",
        )
    except (BotoCoreError, ClientError) as exc:
        log("notification_failed", level="WARNING", order_id=order["id"], error=str(exc))


def fulfil(order):
    """The 'work'. Pretend to pick, pack and confirm the order."""
    time.sleep(PROCESSING_SECONDS)
    if is_poison(order["item"]):
        raise ProcessingError(f"item '{order['item']}' cannot be fulfilled")


# --------------------------------------------------------------------------
# Message handling
# --------------------------------------------------------------------------
def handle_message(sqs, sns, message):
    receive_count = int(message.get("Attributes", {}).get("ApproximateReceiveCount", "1"))
    order_id = None
    try:
        order_id = parse_order_id(message["Body"])
        order = fetch_order(order_id)
        if order is None:
            raise ProcessingError(f"order {order_id} not found")

        # SQS delivers at least once, so the same message can arrive twice. Skip finished orders.
        if order["status"] == "COMPLETED":
            log("duplicate_message_skipped", order_id=order_id)
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
            return

        update_status(order_id, "PROCESSING", receive_count)
        log("order_processing", order_id=order_id, receive_count=receive_count)

        fulfil(order)

        update_status(order_id, "COMPLETED", receive_count)
        send_notification(sns, order)
        sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
        log("order_completed", order_id=order_id, receive_count=receive_count)

    except (ProcessingError, ValueError) as exc:
        status = status_after_failure(receive_count, MAX_RECEIVE_COUNT)
        log(
            "order_failed",
            level="ERROR",
            order_id=order_id,
            receive_count=receive_count,
            status=status,
            error=str(exc),
        )
        if order_id is not None:
            try:
                update_status(order_id, status, receive_count, str(exc))
            except psycopg2.Error as db_exc:
                log("status_update_failed", level="ERROR", order_id=order_id, error=str(db_exc))
        if status == "FAILED":
            log("moving_to_dlq", level="ERROR", order_id=order_id, receive_count=receive_count)
        # Deliberately NOT deleting the message: SQS will redeliver it, or move it to the DLQ.

    except psycopg2.Error as exc:
        # Database trouble is not the order's fault. Leave the message alone so it is retried.
        log("database_error", level="ERROR", order_id=order_id, error=str(exc).strip())


def stop(signum, frame):
    global _running
    log("shutdown_requested", signal=signum)
    _running = False


def main():
    if not SQS_QUEUE_URL:
        raise SystemExit("SQS_QUEUE_URL is not set")

    signal.signal(signal.SIGTERM, stop)  # ECS sends SIGTERM on every deploy
    signal.signal(signal.SIGINT, stop)

    sqs, sns = make_clients()
    log(
        "starting",
        max_receive_count=MAX_RECEIVE_COUNT,
        processing_seconds=PROCESSING_SECONDS,
        sns_enabled=sns is not None,
    )

    while _running:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=WAIT_TIME_SECONDS,
                AttributeNames=["ApproximateReceiveCount"],
            )
        except (BotoCoreError, ClientError) as exc:
            log("receive_failed", level="ERROR", error=str(exc))
            time.sleep(5)
            continue

        for message in response.get("Messages", []):
            if not _running:
                break  # unprocessed messages simply become visible again
            handle_message(sqs, sns, message)

    log("stopped")


if __name__ == "__main__":
    main()

#!/usr/bin/env bash
# Places one order that is built to fail. It should be retried 3 times, end up FAILED,
# and finally land in the dead-letter queue. Usage: ./scripts/place-bad-order.sh [API_URL]
API_URL="${1:-${API_URL:-http://localhost:8001}}"

curl -s -X POST "$API_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"customer_name":"Test Customer","item":"FAULTY-Widget","quantity":1}'
echo

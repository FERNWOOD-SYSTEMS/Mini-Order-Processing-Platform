#!/usr/bin/env bash
# Places a handful of normal orders. Usage: ./scripts/place-orders.sh [API_URL]
# Windows: run it from Git Bash or WSL.
API_URL="${1:-${API_URL:-http://localhost:8001}}"

place() {
  curl -s -X POST "$API_URL/orders" \
    -H "Content-Type: application/json" \
    -d "{\"customer_name\":\"$1\",\"item\":\"$2\",\"quantity\":$3}"
  echo
}

place "Ada"   "Notebook"     2
place "Bola"  "USB-C Cable"  1
place "Chidi" "Laptop Stand" 1
place "Dayo"  "Notebook"     5
place "Efe"   "USB-C Cable"  3

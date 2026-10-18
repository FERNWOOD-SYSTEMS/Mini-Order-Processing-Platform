#!/usr/bin/env bash
# Shows how many messages are in the main queue and in the dead-letter queue (LOCAL ElasticMQ only).
# Needs the AWS CLI. Any credentials work locally, so we default to dummy ones.
# On real AWS, use:  aws sqs get-queue-attributes --queue-url <url> --attribute-names All
ENDPOINT="${SQS_ENDPOINT:-http://localhost:9324}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-local}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-local}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

for q in orders orders-dlq; do
  echo "== $q"
  aws --endpoint-url "$ENDPOINT" sqs get-queue-attributes \
    --queue-url "$ENDPOINT/000000000000/$q" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
    --query "Attributes" --output json
done

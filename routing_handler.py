from __future__ import annotations

import base64
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import boto3
from botocore.exceptions import ClientError


# Structured JSON logger for CloudWatch
class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra"):
            log.update(record.extra)
        return json.dumps(log)

handler = logging.StreamHandler()
handler.setFormatter(_JsonFormatter())
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False


# Config injection from Lambda environment variables
DYNAMODB_TABLE         = os.environ.get("DYNAMODB_TABLE", "route-planning-jobs")
SQS_QUEUE_FULL_REPLAN  = os.environ.get("SQS_QUEUE_FULL_REPLAN", "")
SQS_QUEUE_SINGLE_ORDER = os.environ.get("SQS_QUEUE_SINGLE_ORDER", "")
AWS_REGION             = os.environ.get("AWS_REGION", "eu-west-1")


# Enums and constants
class PlanningType(str, Enum):
    FULL_REPLAN  = "full_replan"
    SINGLE_ORDER = "single_order"

JOB_STATUS_PENDING = "PENDING"
JOB_STATUS_FAILED  = "FAILED"

REQUIRED_FIELDS: dict[PlanningType, list[str]] = {
    PlanningType.FULL_REPLAN:  ["planning_date"],
    PlanningType.SINGLE_ORDER: ["order_id", "planning_date"],
}

ESTIMATED_DURATION: dict[PlanningType, int] = {
    PlanningType.FULL_REPLAN:  3600,
    PlanningType.SINGLE_ORDER: 120,
}


# AWS clients loaded at module-level for warm start which improves performance
_dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
_sqs      = boto3.client("sqs", region_name=AWS_REGION)

def _table():
    return _dynamodb.Table(DYNAMODB_TABLE)


# helper functions
def extract_caller_identity(event: dict[str, Any]) -> dict[str, str]:
    claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
    return {
        "caller_sub":      claims.get("sub", "unknown"),
        "caller_email":    claims.get("email", "unknown"),
        "caller_username": claims.get("cognito:username", "unknown"),
    }

class ValidationError(Exception):
    """Raised when the request body fails validation."""

def validate_body(body: dict[str, Any]) -> PlanningType:
    if not isinstance(body, dict):
        raise ValidationError("Request body must be a JSON object.")

    raw_type = body.get("planning_type")
    if not raw_type:
        raise ValidationError("Missing required field: 'planning_type'.")

    try:
        planning_type = PlanningType(raw_type)
    except ValueError:
        raise ValidationError(f"Invalid 'planning_type': '{raw_type}'.")

    for field in REQUIRED_FIELDS[planning_type]:
        if not body.get(field):
            raise ValidationError(f"Missing field for '{planning_type.value}': '{field}'.")

    planning_date = body.get("planning_date", "")
    try:
        datetime.strptime(planning_date, "%Y-%m-%d")
    except ValueError:
        raise ValidationError(f"Invalid date format (YYYY-MM-DD): '{planning_date}'.")

    return planning_type


# DynamoDB and SQS operations
def persist_job(job_id: str, planning_type: PlanningType, body: dict[str, Any], caller: dict[str, str]):
    now = datetime.now(tz=timezone.utc).isoformat()
    item = {
        "job_id": job_id,
        "planning_type": planning_type.value,
        "status": JOB_STATUS_PENDING,
        "created_at": now,
        "updated_at": now,
        "planning_date": body.get("planning_date"),
        "request_payload": json.dumps(body),
        "estimated_duration_seconds": ESTIMATED_DURATION[planning_type],
        **caller,
        "ttl": int(datetime.now(tz=timezone.utc).timestamp()) + 31536000 # 1 Year
    }
    if planning_type == PlanningType.SINGLE_ORDER:
        item["order_id"] = body.get("order_id")

    _table().put_item(Item=item)

def update_job_status(job_id: str, status: str, error_msg: str = None):
    """Updates the job status in DynamoDB (used for FAILED state rollback)."""
    update_expr = "SET #s = :s, updated_at = :u"
    attr_names = {"#s": "status"}
    attr_vals = {
        ":s": status,
        ":u": datetime.now(tz=timezone.utc).isoformat()
    }
    
    if error_msg:
        update_expr += ", error_message = :e"
        attr_vals[":e"] = error_msg

    try:
        _table().update_item(
            Key={"job_id": job_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=attr_names,
            ExpressionAttributeValues=attr_vals
        )
    except ClientError as e:
        logger.error("Failed to update job status", extra={"job_id": job_id, "error": str(e)})

def enqueue_job(job_id: str, planning_type: PlanningType, body: dict[str, Any], caller: dict[str, str]):
    queue_url = SQS_QUEUE_FULL_REPLAN if planning_type == PlanningType.FULL_REPLAN else SQS_QUEUE_SINGLE_ORDER
    
    message = {
        "job_id": job_id,
        "planning_type": planning_type.value,
        "estimated_duration_seconds": ESTIMATED_DURATION[planning_type],
        **caller,
        **body,
    }

    send_kwargs = {
        "QueueUrl": queue_url,
        "MessageBody": json.dumps(message),
        "MessageAttributes": {
            "planning_type": {"DataType": "String", "StringValue": planning_type.value},
            "job_id": {"DataType": "String", "StringValue": job_id}
        }
    }

    if queue_url.endswith(".fifo"):
        send_kwargs["MessageDeduplicationId"] = job_id
        send_kwargs["MessageGroupId"] = body.get("planning_date", "default")

    _sqs.send_message(**send_kwargs)


# Main lambda handler
def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    caller = extract_caller_identity(event)
    
    # 1. Handle Base64 encoding and JSON parsing
    raw_body = event.get("body", "{}")
    if event.get("isBase64Encoded"):
        raw_body = base64.b64decode(raw_body).decode('utf-8')

    try:
        body = json.loads(raw_body) if isinstance(raw_body, str) else (raw_body or {})
    except json.JSONDecodeError:
        return _response(400, {"error": "Invalid JSON payload."})

    # 2. Validation
    try:
        planning_type = validate_body(body)
    except ValidationError as exc:
        return _response(400, {"error": str(exc)})

    job_id = str(uuid.uuid4())

    # 3. Persist PENDING to DynamoDB
    try:
        persist_job(job_id, planning_type, body, caller)
    except Exception as exc:
        logger.error("DB Persistence failed", extra={"error": str(exc)})
        return _response(500, {"error": "Internal database error."})

    # 4. Enqueue to SQS with Rollback Logic
    try:
        enqueue_job(job_id, planning_type, body, caller)
    except Exception as exc:
        logger.error("SQS Enqueue failed - triggering rollback", extra={"job_id": job_id, "error": str(exc)})
        # Compensating transaction: Update DB to FAILED so polling isn't infinite
        update_job_status(job_id, JOB_STATUS_FAILED, error_msg="Failed to enqueue to SQS.")
        return _response(500, {"error": "Failed to queue job. Please retry."})

    logger.info("Job accepted", extra={"job_id": job_id, "caller": caller["caller_sub"]})

    return _response(202, {
        "job_id": job_id,
        "status": JOB_STATUS_PENDING,
        "estimated_duration_seconds": ESTIMATED_DURATION[planning_type],
        "message": f"Job accepted. Poll GET /plans/{job_id} for status."
    })

def _response(status_code: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
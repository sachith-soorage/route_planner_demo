import json
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

import routing_handler as handler

# Setting up
@pytest.fixture(autouse=True)
def aws_env(monkeypatch):
    monkeypatch.setenv("DYNAMODB_TABLE", "test-jobs")
    monkeypatch.setenv("SQS_QUEUE_FULL_REPLAN", "sample-url-for-testing")
    monkeypatch.setenv("SQS_QUEUE_SINGLE_ORDER", "sample-url-for-testing")


def _make_event(body: dict, base64_encoded=False) -> dict:
    raw = json.dumps(body)
    return {
        "body": raw,
        "isBase64Encoded": base64_encoded,
        "requestContext": {
            "authorizer": {
                "claims": {
                    "sub": "user-123",
                    "email": "sachith@example.com",
                    "cognito:username": "sachith",
                }
            }
        },
    }


# Validation
class TestValidation:
    def test_missing_planning_type(self):
        body = {"planning_date": "2026-01-01"}
        event = _make_event(body)
        resp = handler.lambda_handler(event, None)
        assert resp["statusCode"] == 400
        assert "planning_type" in resp["body"]

    def test_invalid_planning_type(self):
        event = _make_event({"planning_type": "magic_replan", "planning_date": "2026-01-01"})
        resp = handler.lambda_handler(event, None)
        assert resp["statusCode"] == 400

    def test_full_replan_missing_date(self):
        event = _make_event({"planning_type": "full_replan"})
        resp = handler.lambda_handler(event, None)
        assert resp["statusCode"] == 400
        assert "planning_date" in resp["body"]

    def test_single_order_missing_order_id(self):
        event = _make_event({"planning_type": "single_order", "planning_date": "2026-01-01"})
        resp = handler.lambda_handler(event, None)
        assert resp["statusCode"] == 400
        assert "order_id" in resp["body"]

    def test_bad_date_format(self):
        event = _make_event({"planning_type": "full_replan", "planning_date": "01-01-2026"})
        resp = handler.lambda_handler(event, None)
        assert resp["statusCode"] == 400
        assert "YYYY-MM-DD" in resp["body"]

    def test_garbage_json(self):
        event = {"body": "not json at all", "isBase64Encoded": False, "requestContext": {}}
        resp = handler.lambda_handler(event, None)
        assert resp["statusCode"] == 400


# Happy path flow
class TestHappyPath:
    @patch.object(handler, "_sqs")
    @patch.object(handler, "_table", return_value=MagicMock())
    def test_full_replan_accepted(self, mock_table, mock_sqs):
        event = _make_event({"planning_type": "full_replan", "planning_date": "2026-06-01"})
        resp = handler.lambda_handler(event, None)

        assert resp["statusCode"] == 202
        body = json.loads(resp["body"])
        assert body["status"] == "PENDING"
        assert "job_id" in body
        assert body["estimated_duration_seconds"] == 3600

    @patch.object(handler, "_sqs")
    @patch.object(handler, "_table", return_value=MagicMock())
    def test_single_order_accepted(self, mock_table, mock_sqs):
        event = _make_event({
            "planning_type": "single_order",
            "planning_date": "2026-06-01",
            "order_id": "ORD-999"
        })
        resp = handler.lambda_handler(event, None)

        assert resp["statusCode"] == 202
        body = json.loads(resp["body"])
        assert body["estimated_duration_seconds"] == 120


# Failure & rollback
class TestFailureAndRollback:
    @patch.object(handler, "_table")
    def test_db_failure_returns_500(self, mock_table_fn):
        mock_table_fn.return_value.put_item.side_effect = Exception("DynamoDB down")
        event = _make_event({"planning_type": "full_replan", "planning_date": "2026-06-01"})
        resp = handler.lambda_handler(event, None)
        assert resp["statusCode"] == 500
        assert "database" in resp["body"].lower()

    @patch.object(handler, "_sqs")
    @patch.object(handler, "_table")
    def test_sqs_failure_triggers_rollback(self, mock_table_fn, mock_sqs):
        mock_table_instance = MagicMock()
        mock_table_fn.return_value = mock_table_instance
        mock_sqs.send_message.side_effect = Exception("SQS unavailable")

        event = _make_event({"planning_type": "full_replan", "planning_date": "2026-06-01"})
        resp = handler.lambda_handler(event, None)

        assert resp["statusCode"] == 500
        mock_table_instance.update_item.assert_called_once()
        call_args = mock_table_instance.update_item.call_args
        assert ":s" in call_args.kwargs["ExpressionAttributeValues"]
        assert call_args.kwargs["ExpressionAttributeValues"][":s"] == "FAILED"

# Authentication
class TestCallerIdentity:
    def test_missing_claims_defaults_to_unknown(self):
        # no authorizer context at all
        caller = handler.extract_caller_identity({})
        assert caller["caller_sub"] == "unknown"
        assert caller["caller_email"] == "unknown"

    @patch.object(handler, "_sqs")
    @patch.object(handler, "_table")
    def test_caller_stored_in_db(self, mock_table_fn, mock_sqs):
        mock_table_instance = MagicMock()
        mock_table_fn.return_value = mock_table_instance

        event = _make_event({"planning_type": "full_replan", "planning_date": "2026-06-01"})
        handler.lambda_handler(event, None)

        put_item_call = mock_table_instance.put_item.call_args
        item = put_item_call.kwargs["Item"]
        assert item["caller_sub"] == "user-123"
        assert item["caller_email"] == "sachith@example.com"


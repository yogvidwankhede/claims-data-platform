"""Alerting callbacks shared by the claims DAGs.

Every alert is one structured log line (picked up by the log pipeline and routed
to on-call); if CLAIMS_ALERT_WEBHOOK is set, the same payload is POSTed to it
(Slack/PagerDuty/Teams incoming webhook). No webhook is needed to run locally.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

log = logging.getLogger("claims.alerts")


def _send(payload: dict) -> None:
    log.error("ALERT %s", json.dumps(payload, default=str))
    url = os.environ.get("CLAIMS_ALERT_WEBHOOK")
    if url:
        req = urllib.request.Request(
            url, data=json.dumps({"text": payload["summary"], **payload}, default=str).encode(),
            headers={"Content-Type": "application/json"},
        )  # fmt: skip
        urllib.request.urlopen(req, timeout=10)  # noqa: S310 - operator-configured URL


def task_failed(context) -> None:
    ti = context["task_instance"]
    _send({
        "summary": f"claims pipeline: {ti.dag_id}.{ti.task_id} failed for {context.get('ds')}",
        "dag_id": ti.dag_id, "task_id": ti.task_id, "map_index": getattr(ti, "map_index", -1),
        "try_number": ti.try_number, "batch_date": context.get("ds"),
    })  # fmt: skip


def deadline_missed(context=None, **kwargs) -> None:
    """Runs when a DAG run misses its deadline: the marts will be late for the business."""
    _send({"summary": "claims pipeline: daily run missed its deadline; marts will be late", **kwargs})

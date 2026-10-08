import pytest
import time
from unittest.mock import MagicMock, AsyncMock

from pipecat_session_continuity.metrics import (
    InMemoryMetricsExporter,
    PrometheusMetricsExporter,
    OpenTelemetryMetricsExporter,
    CompositeMetricsExporter,
)
from pipecat_session_continuity import SessionContinuity, SessionContinuityManager


def test_in_memory_metrics_exporter_basic():
    exporter = InMemoryMetricsExporter()
    
    # Record checkpoints
    exporter.record_checkpoint(10.0, status="success")
    exporter.record_checkpoint(20.0, status="success")
    exporter.record_checkpoint(30.0, status="error")
    
    # Record resumes
    exporter.record_resume(15.0, is_resumed=True, status="success")
    exporter.record_resume(50.0, is_resumed=False, status="error")
    
    # Record reconnects
    exporter.record_reconnect()
    exporter.record_reconnect()
    
    # Record tool calls
    exporter.record_tool_call("weather", status="pending")
    exporter.record_tool_call("weather", status="completed")
    exporter.record_tool_call("weather", status="duplicate")
    exporter.record_tool_call("calculator", status="pending")

    summary = exporter.get_summary()
    
    assert summary["checkpoints"]["count"] == 3
    assert summary["checkpoints"]["success_count"] == 2
    assert summary["checkpoints"]["error_count"] == 1
    assert summary["checkpoints"]["mean_ms"] == 20.0
    assert summary["checkpoints"]["max_ms"] == 30.0
    assert summary["checkpoints"]["p95_ms"] >= 20.0

    assert summary["resumes"]["count"] == 2
    assert summary["resumes"]["resumed_count"] == 1
    assert summary["resumes"]["fresh_count"] == 0
    assert summary["resumes"]["error_count"] == 1
    assert summary["resumes"]["mean_ms"] == 32.5

    assert summary["reconnects_total"] == 2

    assert summary["tool_calls"]["weather"]["pending"] == 1
    assert summary["tool_calls"]["weather"]["completed"] == 1
    assert summary["tool_calls"]["weather"]["duplicate"] == 1
    assert summary["tool_calls"]["calculator"]["pending"] == 1


def test_prometheus_metrics_exporter():
    try:
        from prometheus_client import CollectorRegistry
    except ImportError:
        pytest.skip("prometheus-client not installed")

    custom_registry = CollectorRegistry()
    exporter = PrometheusMetricsExporter(registry=custom_registry, namespace="test_pipecat")

    exporter.record_checkpoint(12.5, status="success")
    exporter.record_checkpoint(45.0, status="error")
    exporter.record_resume(8.0, is_resumed=True, status="success")
    exporter.record_reconnect()
    exporter.record_tool_call("search", status="duplicate")

    output = exporter.get_latest_str()
    assert "test_pipecat_checkpoint_latency_ms" in output
    assert "test_pipecat_checkpoints_total" in output
    assert "test_pipecat_resume_latency_ms" in output
    assert "test_pipecat_resumes_total" in output
    assert "test_pipecat_reconnects_total" in output
    assert "test_pipecat_tool_calls_total" in output
    assert 'tool_name="search"' in output

    summary = exporter.get_summary()
    assert summary["checkpoints"]["count"] == 2
    assert summary["reconnects_total"] == 1


def test_opentelemetry_metrics_exporter():
    try:
        import opentelemetry.metrics
    except ImportError:
        pytest.skip("opentelemetry-api not installed")

    exporter = OpenTelemetryMetricsExporter(meter_name="test_meter")
    exporter.record_checkpoint(15.0, status="success")
    exporter.record_resume(25.0, is_resumed=True, status="success")
    exporter.record_reconnect()
    exporter.record_tool_call("book_flight", status="completed")

    summary = exporter.get_summary()
    assert summary["checkpoints"]["count"] == 1
    assert summary["resumes"]["count"] == 1
    assert summary["reconnects_total"] == 1
    assert summary["tool_calls"]["book_flight"]["completed"] == 1


def test_composite_metrics_exporter():
    try:
        from prometheus_client import CollectorRegistry
        prom_reg = CollectorRegistry()
        prom_exporter = PrometheusMetricsExporter(registry=prom_reg)
    except ImportError:
        prom_exporter = None

    mem_exporter = InMemoryMetricsExporter()
    exporters = [mem_exporter]
    if prom_exporter:
        exporters.append(prom_exporter)

    composite = CompositeMetricsExporter(exporters)
    composite.record_checkpoint(10.0, status="success")
    composite.record_reconnect()
    composite.record_tool_call("translate", status="duplicate")

    summary = composite.get_summary()
    assert summary["checkpoints"]["count"] == 1
    assert summary["reconnects_total"] == 1
    assert summary["tool_calls"]["translate"]["duplicate"] == 1

    if prom_exporter:
        assert "pipecat_session_reconnects_total" in prom_exporter.get_latest_str()


@pytest.mark.asyncio
async def test_session_continuity_with_prometheus():
    try:
        from prometheus_client import CollectorRegistry
    except ImportError:
        pytest.skip("prometheus-client not installed")

    custom_registry = CollectorRegistry()
    prom_exporter = PrometheusMetricsExporter(registry=custom_registry)

    continuity = SessionContinuity(metrics_exporter=prom_exporter)
    
    # Verify manager initialized with this exporter
    assert continuity.manager.metrics == prom_exporter

    # Save context (checkpoint)
    await continuity.checkpoint(
        context=MagicMock(get_messages=MagicMock(return_value=[{"role": "user", "content": "hi"}])),
        session_id="test-prom-session",
    )

    # Check metrics
    m = continuity.get_metrics()
    assert m["count"] >= 1
    assert "summary" in m
    assert m["summary"]["checkpoints"]["count"] >= 1

    # Prometheus export string
    prom_str = continuity.get_prometheus_metrics_str()
    assert "pipecat_session_checkpoints_total" in prom_str
    assert "pipecat_session_checkpoint_latency_ms" in prom_str

    # Test idempotency metrics recording
    pending_calls = {}
    continuity.record_tool_call(pending_calls, "calc", {"x": 1}, status="pending")
    continuity.complete_tool_call(pending_calls, "calc", 42)
    is_dup, _ = continuity.check_tool_idempotency(pending_calls, "calc", {"x": 1})
    assert is_dup is True

    prom_str_after = continuity.get_prometheus_metrics_str()
    assert 'status="duplicate"' in prom_str_after
    assert 'tool_name="calc"' in prom_str_after


@pytest.mark.asyncio
async def test_session_continuity_resume_metrics():
    continuity = SessionContinuity()
    mock_task = MagicMock()
    mock_task.queue_frames = AsyncMock()
    mock_context = MagicMock()
    mock_context.get_messages.return_value = []

    # First call - fresh session
    is_resumed, _ = await continuity.resume_or_start(mock_task, mock_context, "fresh-session-id")
    assert is_resumed is False
    summary = continuity.get_metrics()["summary"]
    assert summary["resumes"]["count"] == 1
    assert summary["resumes"]["resumed_count"] == 0
    assert summary["reconnects_total"] == 0

    # Save context then resume
    await continuity.checkpoint(
        context=MagicMock(get_messages=MagicMock(return_value=[{"role": "user", "content": "hello"}])),
        session_id="reconnect-session-id",
    )
    is_resumed2, _ = await continuity.resume_or_start(mock_task, mock_context, "reconnect-session-id")
    assert is_resumed2 is True
    summary2 = continuity.get_metrics()["summary"]
    assert summary2["resumes"]["count"] == 2
    assert summary2["resumes"]["resumed_count"] == 1
    assert summary2["reconnects_total"] == 1

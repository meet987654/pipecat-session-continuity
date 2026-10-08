import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class BaseMetricsExporter(ABC):
    """Abstract base class for session continuity metrics exporters."""

    @abstractmethod
    def record_checkpoint(self, duration_ms: float, status: str = "success") -> None:
        """Record the duration and outcome of a context checkpoint."""
        pass

    @abstractmethod
    def record_resume(self, duration_ms: float, is_resumed: bool, status: str = "success") -> None:
        """Record the duration and outcome of a session resume attempt."""
        pass

    @abstractmethod
    def record_reconnect(self) -> None:
        """Record a client reconnect event."""
        pass

    @abstractmethod
    def record_tool_call(self, tool_name: str, status: str) -> None:
        """Record a tool call execution or idempotency interception."""
        pass

    @abstractmethod
    def get_summary(self) -> Dict[str, Any]:
        """Return an in-memory summary dictionary of metrics."""
        pass


class InMemoryMetricsExporter(BaseMetricsExporter):
    """
    Thread-safe in-memory metrics collector.
    Default exporter for lightweight monitoring without external libraries.
    """

    def __init__(self):
        self.checkpoint_times: List[float] = []
        self.checkpoint_success: int = 0
        self.checkpoint_error: int = 0

        self.resume_times: List[float] = []
        self.resume_success: int = 0
        self.resume_error: int = 0
        self.fresh_starts: int = 0

        self.reconnect_count: int = 0
        self.tool_calls: Dict[str, Dict[str, int]] = {}

    def record_checkpoint(self, duration_ms: float, status: str = "success") -> None:
        self.checkpoint_times.append(duration_ms)
        if status == "success":
            self.checkpoint_success += 1
        else:
            self.checkpoint_error += 1

    def record_resume(self, duration_ms: float, is_resumed: bool, status: str = "success") -> None:
        self.resume_times.append(duration_ms)
        if status != "success":
            self.resume_error += 1
        elif is_resumed:
            self.resume_success += 1
        else:
            self.fresh_starts += 1

    def record_reconnect(self) -> None:
        self.reconnect_count += 1

    def record_tool_call(self, tool_name: str, status: str) -> None:
        if tool_name not in self.tool_calls:
            self.tool_calls[tool_name] = {}
        self.tool_calls[tool_name][status] = self.tool_calls[tool_name].get(status, 0) + 1

    def get_summary(self) -> Dict[str, Any]:
        times = sorted(self.checkpoint_times)
        resume_times = sorted(self.resume_times)

        def _stats(t_list: List[float]) -> Dict[str, Any]:
            if not t_list:
                return {"count": 0, "mean_ms": 0.0, "p95_ms": 0.0, "max_ms": 0.0}
            mean = sum(t_list) / len(t_list)
            p95_idx = int(len(t_list) * 0.95)
            if p95_idx >= len(t_list):
                p95_idx = len(t_list) - 1
            return {
                "count": len(t_list),
                "mean_ms": round(mean, 2),
                "p95_ms": round(t_list[p95_idx], 2),
                "max_ms": round(t_list[-1], 2),
            }

        return {
            "checkpoints": {
                **_stats(times),
                "success_count": self.checkpoint_success,
                "error_count": self.checkpoint_error,
            },
            "resumes": {
                **_stats(resume_times),
                "resumed_count": self.resume_success,
                "fresh_count": self.fresh_starts,
                "error_count": self.resume_error,
            },
            "reconnects_total": self.reconnect_count,
            "tool_calls": self.tool_calls,
        }


class PrometheusMetricsExporter(BaseMetricsExporter):
    """
    Exports metrics using prometheus_client with standard labels and histograms.
    """

    def __init__(
        self,
        registry: Optional[Any] = None,
        prefix: str = "pipecat_session",
        namespace: Optional[str] = None,
    ):
        try:
            import prometheus_client
            from prometheus_client import Counter, Histogram
        except ImportError:
            raise ImportError(
                "prometheus_client is required to use PrometheusMetricsExporter. "
                "Install it with `pip install prometheus-client`."
            )

        self.registry = registry or prometheus_client.REGISTRY
        self.prefix = namespace or prefix
        prefix = self.prefix
        self._in_memory = InMemoryMetricsExporter()

        # Checkpoint duration histogram and counter
        self.checkpoint_duration = Histogram(
            f"{prefix}_checkpoint_latency_ms",
            "Latency of session context checkpoint operations in milliseconds",
            ["status"],
            registry=self.registry,
            buckets=(1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0),
        )
        self.checkpoint_total = Counter(
            f"{prefix}_checkpoints_total",
            "Total number of session context checkpoint operations",
            ["status"],
            registry=self.registry,
        )

        # Resume duration histogram and counter
        self.resume_duration = Histogram(
            f"{prefix}_resume_latency_ms",
            "Latency of session resume or start operations in milliseconds",
            ["is_resumed", "status"],
            registry=self.registry,
            buckets=(1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0),
        )
        self.resumes_total = Counter(
            f"{prefix}_resumes_total",
            "Total number of session resume attempts",
            ["is_resumed", "status"],
            registry=self.registry,
        )

        # Reconnect counter
        self.reconnects_total = Counter(
            f"{prefix}_reconnects_total",
            "Total number of client reconnect events handled",
            registry=self.registry,
        )

        # Tool calls counter
        self.tool_calls_total = Counter(
            f"{prefix}_tool_calls_total",
            "Total number of tool calls processed and duplicate checks",
            ["tool_name", "status"],
            registry=self.registry,
        )

    def record_checkpoint(self, duration_ms: float, status: str = "success") -> None:
        self._in_memory.record_checkpoint(duration_ms, status)
        self.checkpoint_duration.labels(status=status).observe(duration_ms)
        self.checkpoint_total.labels(status=status).inc()

    def record_resume(self, duration_ms: float, is_resumed: bool, status: str = "success") -> None:
        self._in_memory.record_resume(duration_ms, is_resumed, status)
        res_label = "true" if is_resumed else "false"
        self.resume_duration.labels(is_resumed=res_label, status=status).observe(duration_ms)
        self.resumes_total.labels(is_resumed=res_label, status=status).inc()

    def record_reconnect(self) -> None:
        self._in_memory.record_reconnect()
        self.reconnects_total.inc()

    def record_tool_call(self, tool_name: str, status: str) -> None:
        self._in_memory.record_tool_call(tool_name, status)
        self.tool_calls_total.labels(tool_name=tool_name, status=status).inc()

    def generate_latest(self) -> bytes:
        """Returns Prometheus exposition text format bytes."""
        import prometheus_client
        return prometheus_client.generate_latest(self.registry)

    def generate_latest_str(self) -> str:
        """Returns Prometheus exposition format as a UTF-8 string."""
        return self.generate_latest().decode("utf-8")

    get_latest_str = generate_latest_str

    def get_summary(self) -> Dict[str, Any]:
        return self._in_memory.get_summary()


class OpenTelemetryMetricsExporter(BaseMetricsExporter):
    """
    Exports metrics using the OpenTelemetry Metrics API.
    """

    def __init__(self, meter: Optional[Any] = None, meter_name: str = "pipecat_session_continuity"):
        try:
            from opentelemetry import metrics
        except ImportError:
            raise ImportError(
                "opentelemetry-api is required to use OpenTelemetryMetricsExporter. "
                "Install it with `pip install opentelemetry-api`."
            )

        if meter is None:
            meter = metrics.get_meter(meter_name)
        self.meter = meter
        self._in_memory = InMemoryMetricsExporter()

        self.checkpoint_duration = self.meter.create_histogram(
            name="pipecat.session.checkpoint.duration",
            unit="ms",
            description="Latency of session context checkpoint operations in milliseconds",
        )
        self.checkpoint_counter = self.meter.create_counter(
            name="pipecat.session.checkpoints",
            unit="1",
            description="Total count of checkpoint operations",
        )

        self.resume_duration = self.meter.create_histogram(
            name="pipecat.session.resume.duration",
            unit="ms",
            description="Latency of session resume or start operations in milliseconds",
        )
        self.resume_counter = self.meter.create_counter(
            name="pipecat.session.resumes",
            unit="1",
            description="Total count of session resume attempts",
        )

        self.reconnect_counter = self.meter.create_counter(
            name="pipecat.session.reconnects",
            unit="1",
            description="Total count of client reconnect events",
        )

        self.tool_call_counter = self.meter.create_counter(
            name="pipecat.session.tool_calls",
            unit="1",
            description="Total count of tool call executions and idempotency events",
        )

    def record_checkpoint(self, duration_ms: float, status: str = "success") -> None:
        self._in_memory.record_checkpoint(duration_ms, status)
        attrs = {"status": status}
        self.checkpoint_duration.record(duration_ms, attributes=attrs)
        self.checkpoint_counter.add(1, attributes=attrs)

    def record_resume(self, duration_ms: float, is_resumed: bool, status: str = "success") -> None:
        self._in_memory.record_resume(duration_ms, is_resumed, status)
        attrs = {"is_resumed": is_resumed, "status": status}
        self.resume_duration.record(duration_ms, attributes=attrs)
        self.resume_counter.add(1, attributes=attrs)

    def record_reconnect(self) -> None:
        self._in_memory.record_reconnect()
        self.reconnect_counter.add(1)

    def record_tool_call(self, tool_name: str, status: str) -> None:
        self._in_memory.record_tool_call(tool_name, status)
        attrs = {"tool_name": tool_name, "status": status}
        self.tool_call_counter.add(1, attributes=attrs)

    def get_summary(self) -> Dict[str, Any]:
        return self._in_memory.get_summary()


class CompositeMetricsExporter(BaseMetricsExporter):
    """
    Fans out metrics calls to multiple exporters simultaneously.
    """

    def __init__(self, exporters: List[BaseMetricsExporter]):
        self.exporters = exporters

    def record_checkpoint(self, duration_ms: float, status: str = "success") -> None:
        for exp in self.exporters:
            try:
                exp.record_checkpoint(duration_ms, status)
            except Exception as e:
                logger.warning(f"Error in metrics exporter {exp}: {e}")

    def record_resume(self, duration_ms: float, is_resumed: bool, status: str = "success") -> None:
        for exp in self.exporters:
            try:
                exp.record_resume(duration_ms, is_resumed, status)
            except Exception as e:
                logger.warning(f"Error in metrics exporter {exp}: {e}")

    def record_reconnect(self) -> None:
        for exp in self.exporters:
            try:
                exp.record_reconnect()
            except Exception as e:
                logger.warning(f"Error in metrics exporter {exp}: {e}")

    def record_tool_call(self, tool_name: str, status: str) -> None:
        for exp in self.exporters:
            try:
                exp.record_tool_call(tool_name, status)
            except Exception as e:
                logger.warning(f"Error in metrics exporter {exp}: {e}")

    def get_summary(self) -> Dict[str, Any]:
        if self.exporters:
            return self.exporters[0].get_summary()
        return {}

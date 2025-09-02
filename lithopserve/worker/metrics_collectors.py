import time
import logging
import psutil
from lithopserve.worker.metrics import CPUMetric, MemoryMetric, DiskMetric, NetworkMetric

logger = logging.getLogger(__name__)


# Decorator to handle psutil.NoSuchProcess error
def handle_psutil_error(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (psutil.NoSuchProcess, psutil.AccessDenied, PermissionError) as e:
            logger.warning("Process no longer exists.")
            return None
    return wrapper


# Base interface for metric collectors
class IMetricCollector:
    def collect_metric(self, pid=None, parent_pid=None, collection_id=None):
        current_time = time.time()  # Capture current time
        return self._collect(pid, parent_pid, current_time, collection_id)

    def _collect(self, pid, parent_pid, timestamp, collection_id):
        raise NotImplementedError

    # Moved _calculate_rates method here so all collectors can use it
    def _calculate_rates(self, current_value, previous_value, time_diff):
        return (current_value - previous_value) / time_diff if time_diff > 0 else 0


# Base class for metric collectors including metric storage
class BaseMetricCollector(IMetricCollector):
    def __init__(self, metric_class):
        self.metrics = []  # Stores collected metrics
        self.metric_class = metric_class

    # Find the previous metric for a process
    def _find_previous_metric(self, pid):
        return next((metric for metric in reversed(self.metrics) if metric.pid == pid), None)


# Base class for global metric collectors (e.g., Network)
class GlobalMetricCollector(IMetricCollector):
    def __init__(self, metric_class):
        self.metrics = []  # Stores collected metrics
        self.metric_class = metric_class

    # Overriding collect_metric to ignore pid and parent_pid
    def collect_metric(self, collection_id):
        timestamp = time.time()  # Capture current time
        return self._collect(timestamp, collection_id)

    def _collect(self, timestamp, collection_id):
        raise NotImplementedError

    # Find the previous metric for global metrics
    def _find_previous_metric(self):
        return self.metrics[-1] if self.metrics else None


# CPU Metric Collector
class CPUMetricCollector(BaseMetricCollector):
    def __init__(self):
        super().__init__(CPUMetric)

    @handle_psutil_error
    def _collect(self, pid, parent_pid, timestamp, collection_id):
        process = psutil.Process(pid)
        cpu_usage = process.cpu_percent(interval=0.01)
        cpu_times = process.cpu_times()

        cpu_metric = CPUMetric(
            timestamp=timestamp,
            pid=pid,
            parent_pid=parent_pid,
            cpu_usage=cpu_usage,
            user_time=cpu_times.user,
            system_time=cpu_times.system,
            children_user_time=getattr(cpu_times, 'children_user', 0),
            children_system_time=getattr(cpu_times, 'children_system', 0),
            iowait_time=getattr(cpu_times, 'iowait', 0),
            collection_id=collection_id
        )
        self.metrics.append(cpu_metric)
        return cpu_metric


# Memory Metric Collector
class MemoryMetricCollector(BaseMetricCollector):
    def __init__(self):
        super().__init__(MemoryMetric)

    @handle_psutil_error
    def _collect(self, pid, parent_pid, timestamp, collection_id):
        memory_usage = psutil.Process(pid).memory_info().rss >> 20  # Convert to MB
        memory_metric = MemoryMetric(
            timestamp=timestamp,
            pid=pid,
            parent_pid=parent_pid,
            memory_usage=memory_usage,
            collection_id=collection_id
        )
        self.metrics.append(memory_metric)
        return memory_metric


# Disk Metric Collector
class DiskMetricCollector(BaseMetricCollector):
    def __init__(self):
        super().__init__(DiskMetric)

    @handle_psutil_error
    def _collect(self, pid, parent_pid, timestamp, collection_id):
        io_counters = psutil.Process(pid).io_counters()
        disk_read_mb = io_counters.read_bytes / 1024.0**2
        disk_write_mb = io_counters.write_bytes / 1024.0**2

        prev_metric = self._find_previous_metric(pid)

        if prev_metric is None:
            disk_read_rate = 0
            disk_write_rate = 0
        else:
            time_diff = timestamp - prev_metric.timestamp
            disk_read_rate = self._calculate_rates(disk_read_mb, prev_metric.disk_read_mb, time_diff)
            disk_write_rate = self._calculate_rates(disk_write_mb, prev_metric.disk_write_mb, time_diff)

        disk_metric = DiskMetric(
            timestamp=timestamp,
            pid=pid,
            parent_pid=parent_pid,
            disk_read_mb=disk_read_mb,
            disk_write_mb=disk_write_mb,
            disk_read_rate=disk_read_rate,
            disk_write_rate=disk_write_rate,
            collection_id=collection_id
        )
        self.metrics.append(disk_metric)
        return disk_metric


# Network Metric Collector
class NetworkMetricCollector(GlobalMetricCollector):
    def __init__(self):
        super().__init__(NetworkMetric)

    @handle_psutil_error
    def _collect(self, timestamp, collection_id):
        # Collect global network metrics (not per-process)
        net_counters = psutil.net_io_counters(pernic=False)
        net_read_mb = net_counters.bytes_recv / 1024.0**2
        net_write_mb = net_counters.bytes_sent / 1024.0**2

        prev_metric = self._find_previous_metric()
        if prev_metric is None:
            net_read_rate = 0
            net_write_rate = 0
        else:
            time_diff = timestamp - prev_metric.timestamp
            net_read_rate = self._calculate_rates(net_read_mb, prev_metric.net_read_mb, time_diff)
            net_write_rate = self._calculate_rates(net_write_mb, prev_metric.net_write_mb, time_diff)

        network_metric = NetworkMetric(
            timestamp=timestamp,
            net_read_mb=net_read_mb,
            net_write_mb=net_write_mb,
            net_read_rate=net_read_rate,
            net_write_rate=net_write_rate,
            collection_id=collection_id
        )
        self.metrics.append(network_metric)
        return network_metric


class MetricCollector:
    def __init__(self):
        self.cpu_collector = CPUMetricCollector()
        self.memory_collector = MemoryMetricCollector()
        self.disk_collector = DiskMetricCollector()
        self.network_collector = NetworkMetricCollector()

        # Link metrics to the collectors
        self.cpu_metrics = self.cpu_collector.metrics
        self.memory_metrics = self.memory_collector.metrics
        self.disk_metrics = self.disk_collector.metrics
        self.network_metrics = self.network_collector.metrics

    def collect_all_metrics(self, parent_pid, index):
        process_list = self._get_process_list(parent_pid)

        for proc in process_list:
            pid = proc.pid
            parent_pid = proc.ppid()
            self._collect_metrics_for_process(pid, parent_pid, index)

        # Collect global network metrics
        self.network_collector.collect_metric(index)

    def _get_process_list(self, parent_pid):
        current_process = psutil.Process(parent_pid)
        children = current_process.children(recursive=True)
        return [current_process] + children

    def _collect_metrics_for_process(self, pid, parent_pid, index):
        self.memory_collector.collect_metric(pid, parent_pid, index)
        self.cpu_collector.collect_metric(pid, parent_pid, index)
        self.disk_collector.collect_metric(pid, parent_pid, index)

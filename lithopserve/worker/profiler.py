import logging
import queue
import random
import time
import psutil
import traceback
from lithopserve.worker.metrics_collectors import MetricCollector

logger = logging.getLogger(__name__)


class Profiler:
    """
    Class for profiling a worker process, collecting system metrics (CPU, memory, disk, network),
    and sending them to Prometheus for monitoring.
    """

    METRIC_COLLECTIONS_FIELDS = {
        'cpu_metrics': ['cpu_usage', 'user_time', 'system_time', 'children_user_time', 'children_system_time', 'iowait_time'],
        'memory_metrics': ['memory_usage'],
        'disk_metrics': ['disk_read_mb', 'disk_write_mb', 'disk_read_rate', 'disk_write_rate'],
        'network_metrics': ['net_read_mb', 'net_write_mb', 'net_read_rate', 'net_write_rate'],
    }

    def __init__(self):
        self.worker_id = None
        self.worker_start_tstamp = None
        self.worker_end_tstamp = None
        self.metrics = MetricCollector()  # To gather system metrics
        self.metric_queue = queue.Queue()  # Temporary storage for metrics

    def start_profiling(self, conn, monitored_process_pid, prometheus, job, profiler_timeout=10):
        """
        Start collecting system metrics for the monitored process and send the data to Prometheus.
        """
        index = 0
        profiler_timeout = profiler_timeout or 10  # Use a default timeout if none provided
        logger.debug(f"Profiler timeout set to {profiler_timeout} seconds")

        self._initialize_process(monitored_process_pid)
        profiling_data = self._prepare_profiling_data(job)

        try:
            while True:
                start_time = time.time()

                # Collect and send metrics
                try:
                    self._collect_and_send_metrics(prometheus, profiling_data, monitored_process_pid, index)
                    index += 1

                    # Handle stop signal from the connection
                    if self._check_stop_signal(conn):
                        break

                    # Sleep until the next collection interval
                    self._sleep_until_next_interval(start_time, profiler_timeout)

                except psutil.NoSuchProcess:
                    logger.warning("Monitored process no longer exists, stopping data collection.")
                    break

        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(f"Exception in profiling process: {e}")
        finally:
            conn.close()

    def _initialize_process(self, monitored_process_pid):
        """
        Initialize the process and log worker details.
        """
        self.process = psutil.Process(monitored_process_pid)
        self.process.cpu_percent()  # Initialize CPU tracking

    def _prepare_profiling_data(self, job):
        """
        Prepare metadata for profiling (job details, etc.).
        """
        return {
            'job_id': job.job_id,
            'executor_id': job.executor_id,
            'call_id': job.call_id,
        }

    def _collect_metrics(self, monitored_process_pid, index):
        """
        Collects all metrics for the given process and index.
        """
        try:
            self.metrics.collect_all_metrics(monitored_process_pid, index)
        except psutil.NoSuchProcess:
            logger.warning(f"Process {monitored_process_pid} does not exist. Skipping metric collection.")
            return

    def _send_metrics(self, prometheus, profiling_data, index):
        """
        Sends the collected metrics to Prometheus.
        """
        for metric_type, keys in self.METRIC_COLLECTIONS_FIELDS.items():
            try:
                metric_list = getattr(self.metrics, metric_type, [])
                if index < len(metric_list):
                    metric = metric_list[index]
                    self._send_metrics_to_prometheus(prometheus, metric, metric_type, keys, profiling_data)
            except Exception as e:
                logger.error(f"Error while processing {metric_type}: {e}")

    def _collect_and_send_metrics(self, prometheus, profiling_data, monitored_process_pid, index):
        """
        Collect system metrics and send them to Prometheus.
        """
        self._collect_metrics(monitored_process_pid, index)
        self._send_metrics(prometheus, profiling_data, index)

    def _send_metrics_to_prometheus(self, prometheus, metric, metric_type, keys, profiling_data):
        """
        Send the collected metrics to Prometheus with proper labeling.
        """
        for key in keys:
            metric_value = getattr(metric, key, None)
            if metric_value is not None:
                try:
                    metric_value = float(metric_value)
                except (ValueError, TypeError):
                    logger.warning(f"Skipping metric {key}: expected float, got {metric_value}")
                    continue  # Skip sending this particular metric

                labels_dict = profiling_data.copy()
                if metric_type != 'network_metrics':
                    labels_dict.update({'pid': str(metric.pid), 'parent_pid': str(metric.parent_pid)})

                print(f"[DEBUG] Sending to Prometheus: metric={key} value={metric_value} labels={labels_dict}")
                self.send_metric_to_prometheus(prometheus, key, metric_value, 'gauge', labels_dict)

    def _check_stop_signal(self, conn):
        """
        Check if a stop signal is received via the connection.
        """
        if conn.poll():
            message = conn.recv()
            if message == "stop":
                logger.debug("Received stop signal, completing current data collection.")
                return True
        return False

    def _sleep_until_next_interval(self, start_time, profiler_timeout):
        """
        Calculate sleep time and wait until the next profiling interval.
        """
        elapsed_time = time.time() - start_time
        random_delay = random.uniform(0, 1)  # To avoid Prometheus requests peaks
        sleep_time = max(0, profiler_timeout - elapsed_time + random_delay)
        time.sleep(sleep_time)

    def send_metric_to_prometheus(self, prometheus, key, value, metric_type, labels_dict):
        """
        Send the collected metric to Prometheus for monitoring.
        """
        try:
            prometheus.send_metric(
                name=key,
                value=value,
                type=metric_type,
                labels=labels_dict
            )
        except Exception as e:
            logger.error(f"Failed to send {key} metric to Prometheus: {e}")

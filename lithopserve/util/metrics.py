import requests
import logging
import os

logger = logging.getLogger(__name__)


class PrometheusExporter():

    def __init__(self, enabled, config):
        """ Prometheus exporter for sending metrics to an API Gateway"""
        self.enabled = enabled
        self.apigateway = config.get('apigateway') if config else None

        self.job = 'lithopserve'
        self.instance = os.environ['__LITHOPS_SESSION_ID'].split('-')[0]

    def send_metric(self, name, value, type, labels, timestamp=None):
        """Send a metric to Prometheus with optional timestamp"""
        if self.enabled and self.apigateway:
            if not isinstance(value, (int, float)):
                logger.error(f"Failed to send metric: {name} - expected float as value, got {value}")
                return

            dim = 'job/{}/instance/{}'.format(self.job, self.instance)
            if isinstance(labels, dict):
                items = labels.items()
            elif isinstance(labels, (tuple, list)):
                items = labels
            else:
                raise TypeError("Labels should be a dict or a list/tuple of tuples")

            for key, val in items:
                dim += '/%s/%s' % (key, val)
            url = '/'.join([self.apigateway, 'metrics', dim])
            payload = '# TYPE %s %s\n' % (name, type)
            metric_line = '%s %s' % (name, value)
            if timestamp:
                # Convert timestamp to milliseconds (Prometheus expects UNIX timestamp in milliseconds)
                timestamp_ms = int(timestamp * 1000)
                metric_line += ' %d' % timestamp_ms
            payload += metric_line + '\n'

            try:
                response = requests.post(url, data=payload, headers={"Content-Type": "text/plain"})
                if response.status_code != 200:
                    logger.error(f'Failed to send metric: {response.text}')
            except Exception as e:
                logger.error(e)

    def delete_metric(self, name, labels):
        """Delete a metric from the API Gateway/Pushgateway for the given label set"""
        if self.enabled and self.apigateway:
            dim = 'job/{}/instance/{}'.format(self.job, self.instance)
            if isinstance(labels, dict):
                items = labels.items()
            elif isinstance(labels, (tuple, list)):
                items = labels
            else:
                raise TypeError("Labels should be a dict or a list/tuple of tuples")

            for key, val in items:
                dim += '/%s/%s' % (key, val)
            # URL is the same structure as send_metric
            url = '/'.join([self.apigateway, 'metrics', dim])
            try:
                response = requests.delete(url)
                if response.status_code != 202 and response.status_code != 200:
                    logger.error(f'Failed to delete metric: {response.text}')
            except Exception as e:
                logger.error(e)
"""
Outlier Detection using Tukeys Filter Class
"""
import sys
import itertools
from time import time

sys.path.append('../')

from lib.modules.base_task import BaseTask
from lib.modules.helper import extract_service_name, get_closest_datapoint
from lib.modules.models import TimeSeriesTuple


class TukeysFilter(BaseTask):

    def __init__(self, config, logger, options):
        super(TukeysFilter, self).__init__(config, logger, resource={'metric_store': 'RedisSink',
                                                             'sink': 'GraphiteSink'})
        self.plugin = options['plugin']
        self.service = options['service']
        self.params = options['params']

    def read(self):
        quantile_25 = self.params['quantile_25']
        quantile_75 = self.params['quantile_75']
        metrics = self.params['metrics']
        delay = self.params.get('delay', 60)

        # read metrics from metric_store
        quantile_25 = [i for i in self.metric_store.read(quantile_25)]
        quantile_75 = [i for i in self.metric_store.read(quantile_75)]
        metrics = [i for i in self.metric_store.read(metrics)]
        if not (len(quantile_25) * len(quantile_75) * len(metrics)):
                self.logger.error('No data found for quantile/to be checked metrics. Exiting')
                return None

        # sort TimeSeriesTuples by timestamp
        quantile_25 = sorted(quantile_25, key=lambda tup: tup.timestamp)
        quantile_75 = sorted(quantile_75, key=lambda tup: tup.timestamp)
        metrics = sorted(metrics, key=lambda tup: (tup.name, tup.timestamp))

        # find closest datapoint to now() (corrected by delay) if not too old
        time_now = time() - delay
        quantile_25 = get_closest_datapoint(quantile_25, time_now)
        if time_now - quantile_25.timestamp > 600:
            self.logger.error('Quantile25 Value is too old (%d sec). Exiting' % (time_now - quantile_25.timestamp))
            return None
        quantile_25 = quantile_25.value
        quantile_75 = get_closest_datapoint(quantile_75, time_now)
        if time_now - quantile_75.timestamp > 600:
            self.logger.error('Quantile75 Value is too old (%d sec). Exiting' % (time_now - quantile_75.timestamp))
            return None
        quantile_75 = quantile_75.value
        if quantile_25 > quantile_75:
            self.logger.error('Inconsistent Quantile Values. Exiting')
            return None

        # group by metric (e.g. instance) first and find then closest datapoint
        distribution = {}
        grouped = itertools.groupby(metrics, key=lambda tup: tup.name)
        for key, metrics in grouped:
            closest_datapoint = get_closest_datapoint([metric for metric in metrics], time_now)
            if time_now - closest_datapoint.timestamp < 600:
                distribution[key] = closest_datapoint.value
        if len(distribution) == 0:
            self.logger.error('No Distribution Values. Exiting')
            return None

        return quantile_25, quantile_75, distribution

    def process(self, data):

        quantile_25, quantile_75, distribution = data
        iqr_scaling = self.params.get('iqr_scaling', 1.5)

        iqr = quantile_75 - quantile_25
        lower_limit = quantile_25 - iqr_scaling * iqr
        upper_limit = quantile_75 + iqr_scaling * iqr

        if 'static_lower_threshold' in self.params:
            lower_limit = max(lower_limit, self.params['static_lower_threshold'])
        if 'static_upper_threshold' in self.params:
            upper_limit = min(upper_limit, self.params['static_upper_threshold'])

        states = {}
        for metric, value in distribution.iteritems():
            if value > upper_limit:
                states[metric] = 1.0
            elif value < lower_limit:
                states[metric] = -1.0
            else:
                states[metric] = 0.0

        return quantile_25, quantile_75, states

    def write(self, data):
        quantile_25, quantile_75, states = data
        prefix = '%s.%s' % (self.plugin, self.service)
        count = len(states)
        invalid = 0
        now = int(time())
        for name, state in states.iteritems():
            if state:
                invalid += 1
            name = extract_service_name(name)
            self.sink.write(TimeSeriesTuple('%s.%s' % (prefix, name), now, state))

        self.sink.write(TimeSeriesTuple('%s.%s' % (prefix, 'quantile_25'), now, quantile_25))
        self.sink.write(TimeSeriesTuple('%s.%s' % (prefix, 'quantile_75'), now, quantile_75))
        self.sink.write(TimeSeriesTuple('%s.%s' % (prefix, 'count'), now, count))
        self.sink.write(TimeSeriesTuple('%s.%s' % (prefix, 'invalid'), now, invalid))

    def run(self):
        data = self.read()
        if data:
            state = self.process(data)
            self.write(state)
            return True
        else:
            return None

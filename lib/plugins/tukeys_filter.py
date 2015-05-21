"""
Outlier Detection using Tukeys Filter Class
"""
import re

from collections import defaultdict
from twitter.common import log

from lib.modules.base_task import BaseTask


class TukeysFilter(BaseTask):

    def __init__(self, config, options):
        super(TukeysFilter, self).__init__(config, resource={'data_store': 'RedisSink',
                                                             'output_sink': 'GraphiteSink'})
        self.plugin = options['plugin']
        self.service = options['service']
        self.params = options['params']
        self.unique_keys = options['unique_keys']
        # TO DO
        # logging?

    def _get_valid_value(self, valid_entry, metrics):
        metric = [sorted(metrics)[-valid_entry]]
        return self.data_store.read(metric)

    def _extract_service_name(self, name):
        # TO DO
        # how to?
        # name = name.split('.')[1]
        # name = name.split(':')[1]
        return name

    def read(self):
        q25 = self.params['q25']
        q75 = self.params['q75']
        metrics_regex = self.params['metrics_regex']
        default = self.params['default']

        q25_metrics = []
        q75_metrics = []
        distribution_metrics = []
        for metric in self.unique_keys:
            if q25 in metric:
                q25_metrics.append(metric)
            elif q75 in metric:
                q75_metrics.append(metric)
            # elif re.match(metrics_regex + '$', metric):
            elif re.match(metrics_regex, metric):
                distribution_metrics.append(metric)

        try:
            q25_val = self._get_valid_value(2, q25_metrics)
            q75_val = self._get_valid_value(2, q75_metrics)
            distribution = defaultdict(lambda: default)
            data = self.data_store.read(distribution_metrics)
            distribution = defaultdict(lambda: default, [(metric, float(data[el]))
                                       for el, metric in enumerate(distribution_metrics)
                                       if data[el]])
        except Exception as e:
            return

        return q25_val, q75_val, distribution

    def process(self, data):

        q25_val, q75_val, distribution = data
        iqr_scaling = self.params['iqr_scaling']

        if q25_val > q75_val:
            return False
        if len(distribution) == 0:
            return False

        iqr = q75_val - q25_val
        lower_limit = q25_val - iqr_scaling * iqr
        upper_limit = q75_val + iqr_scaling * iqr

        if 'static_upper_threshold' in self.params:
            lower_limit = max(lower_limit, self.params['static_lower_threshold'])
            upper_limit = min(upper_limit, self.params['static_upper_threshold'])

        distribution_states = {}
        for metric, value in distribution.iteritems():
            if value > upper_limit:
                distribution_states[metric] = 1.0
            elif value < lower_limit:
                distribution_states[metric] = -1.0
            else:
                distribution_states[metric] = 0.0

        return q25_val, q75_val, distribution_states

    def evaluate(self, data):
        return data

    def write(self, data):
        q25_val, q75_val, states = data
        prefix = '%s.%s' % (self.plugin, self.service)
        count = len(states)
        invalid = 0
        for name, state in states.iteritems():
            if state:
                invalid += 1
            name = self._extract_service_name(name)
            self.output_sink.write(prefix + '.' + name, state)

        self.output_sink.write(prefix + '.q25', q25_val)
        self.output_sink.write(prefix + '.q75', q75_val)
        self.output_sink.write(prefix + '.count', count)
        self.output_sink.write(prefix + '.invalid', invalid)

    def run(self):
        data = self.read()
        output = self.process(data)
        state = self.evaluate(output)
        self.write(state)

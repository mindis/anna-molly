"""
Seasonal Decomposition Class
"""
import json
import random
import rpy2.robjects as robjects

from numpy import asarray, median
from twitter.common import log

from lib.modules.base_task import BaseTask
from lib.plugins.tdigest import TDigest


class SeasonalDecomposition(BaseTask):

    def __init__(self, config, options):
        super(SeasonalDecomposition, self).__init__(config, resource={'data_store': 'RedisSink',
                                                                      'output_sink': 'GraphiteSink'})
        self.plugin = options['plugin']
        self.service = options['service']
        self.params = options['options']
        self.unique_keys = options['unique_keys']
        self.tdigest_key = 'td:%s' % self.service
        self.td = TDigest()
        tdigest_json = self.data_store.read([self.tdigest_key])
        if tdigest_json:
            tdigest = json.loads(tdigest_json)
            [self.td.add(c[0], c[1]) for c in tdigest['centroids']]
        self.error_eval = {
            'tukey': self._eval_tukey,
            'quantile': self._eval_quantile
        }

    def _build_tdigest(self, error):
        error_history_length = self.params['error_history_length']
        td = TDigest()
        try:
            metric_data = self.output_sink.read(metric, error_history_length)
            error_data = [val[1] for val in metric_data if val[1]]
            [td.add(error, 1.0) for error in error_data]
            td.compress()
        except:
            td.add(error, 1.0)
        return td

    def _eval_quantile(self, error):
        state = {}
        alpha = self.params['alpha']
        lower = self.td.getValueAtQuantileX(alpha / 2)
        upper = self.td.getValueAtQuantileX(1 - alpha / 2)
        bin_error = 0
        if error >= upper:
            bin_error = 1
        elif error <= lower:
            bin_error = -1
        state['error'] = bin_error
        state['lower'] = lower
        state['upper'] = upper
        state['alpha'] = alpha
        return state

    def _eval_tukey(self, error):
        state = {}
        scaling = self.params['error_params']['scaling']
        q25 = self.td.quantile(0.25)
        q75 = self.td.quantile(0.75)
        iqr = q75 - q25
        lower = q25 - scaling * iqr
        upper = q75 + scaling * iqr
        bin_error = 0
        if error >= upper:
            bin_error = 1
        elif error <= lower:
            bin_error = -1
        state['error'] = bin_error
        state['lower'] = lower
        state['upper'] = upper
        return state

    def read(self):
        name = self.params['metric']
        period_length = self.params['period_length']
        seasons = self.params['seasons']
        default = self.params['default']
        metrics = [metric for metric in self.unique_keys if name in metric]
        if len(metrics) < period_length * seasons:
            return

        try:
            data_raw = self.data_store.read(sorted(metrics))
            data = [float(val) if val else float(default) for val in data_raw]
            data = data[-period_length * seasons - 1:-1]
        except Exception as e:
            return

        return data

    def process(self, data):
        period_length = self.params['period_length']
        error_type = self.params['error_type']

        r_stl = robjects.r.stl
        r_ts = robjects.r.ts
        r_data_ts = r_ts(data, frequency=period_length)
        r_res = r_stl(r_data_ts, s_window="periodic", robust=True)
        r_res_ts = asarray(r_res[0])
        seasonal = r_res_ts[:, 0][-1]
        trend = r_res_ts[:, 1][-1]
        _error = r_res_ts[:, 2][-1]
        model = seasonal + trend
        # DEBUG
        # model = 1
        # seasonal = 1
        # trend = 0
        # error = random.random()

        if error_type == 'normed_error':
            error = _error / model if model != 0 else -1
        elif error_type == 'median_avg_error':
            error = data[-1] - seasonal - median(data)
        elif error_type == 'stl_error':
            error = _error

        return seasonal, trend, error

    def evaluate(self, data):
        seasonal, trend, error = data
        self.td.add(error, 1.0)
        state = self.error_eval[self.params['error_handling']](error)
        return state

    def write(self, state):
        prefix = '%s.%s' % (self.plugin, self.service)
        for name, value in state.iteritems():
            metric_name = '%s.%s' % (prefix, name)
            self.output_sink.write(metric_name, value)
        self.data_store.write_obj(self.tdigest_key, self.td.serialize())

    def run(self):
        data = self.read()
        output = self.process(data)
        state = self.evaluate(output)
        self.write(state)

"""
Seasonal Decomposition Class
"""
import json
import rpy2.robjects as robjects

from numpy import median, asarray

from lib.modules.base_task import BaseTask
from lib.plugins.tdigest import TDigest
from lib.modules.helper import find_step_size, insert_missing_datapoints


class SeasonalDecomposition(BaseTask):

    def __init__(self, config, options):
        super(SeasonalDecomposition, self).__init__(config, resource={'metric_store': 'RedisSink',
                                                                      'sink': 'GraphiteSink'})
        self.plugin = options['plugin']
        self.service = options['service']
        self.params = options['params']
        self.tdigest_key = 'td:%s' % self.service
        self.td = TDigest()
        self.error_eval = {
            'tukey': self._eval_tukey,
            'quantile': self._eval_quantile
        }

    # def _build_tdigest(self, error):
    #     error_history_length = self.params['error_history_length']
    #     td = TDigest()
    #     try:
    #         metric_data = self.sink.read(metric, error_history_length)
    #         error_data = [val[1] for val in metric_data if val[1]]
    #         [td.add(error, 1.0) for error in error_data]
    #         td.compress()
    #     except:
    #         pass
    #     return td

    def _eval_quantile(self, error):
        state = {}
        alpha = self.params['alpha']
        lower = self.td.quantile(alpha / 2)
        upper = self.td.quantile(1 - alpha / 2)
        flag = 0
        if error >= upper:
            flag = 1
        elif error <= lower:
            flag = -1
        state['flag'] = flag
        state['lower'] = lower
        state['upper'] = upper
        state['alpha'] = alpha
        return state

    def _eval_tukey(self, error):
        state = {}
        iqr_scaling = self.params['error_params'].get('iqr_scaling', 1.5)
        quantile_25 = self.td.quantile(0.25)
        quantile_75 = self.td.quantile(0.75)
        iqr = quantile_75 - quantile_25
        lower = quantile_25 - iqr_scaling * iqr
        upper = quantile_75 + iqr_scaling * iqr
        flag = 0
        if error >= upper:
            flag = 1
        elif error <= lower:
            flag = -1
        state['flag'] = flag
        state['lower'] = lower
        state['upper'] = upper
        return state

    def read(self):
        metric = self.params['metric']
        period_length = self.params['period_length']
        seasons = self.params['seasons']
        default = self.params['default']

        tdigest_json = self.metric_store.read(self.tdigest_key)
        if tdigest_json:
            centroids = json.loads(tdigest_json)
            [self.td.add(c[0], c[1]) for c in centroids]

        # gather data and fill with defaults if necessary
        data = self.metric_store.read(metric)
        data = sorted(data, key=lambda tup: tup.timestamp)
        step_size = find_step_size(data)
        data = insert_missing_datapoints(data, default, step_size)

        if len(data) < period_length * seasons:
            self.logger.error('Not enough Datapoints. Exiting')
            return None

        data = data[-period_length * seasons - 1:-1]
        return data

    def process(self, data):
        period_length = self.params['period_length']
        error_type = self.params['error_type']

        try:
            r_stl = robjects.r.stl
            r_ts = robjects.r.ts
            r_data_ts = r_ts(data, frequency=period_length)
            r_res = r_stl(r_data_ts, s_window="periodic", robust=True)
            r_res_ts = asarray(r_res[0])
            seasonal = r_res_ts[:, 0][-1]
            trend = r_res_ts[:, 1][-1]
            _error = r_res_ts[:, 2][-1]
            model = seasonal + trend
        except Exception as e:
            self.logger.error('STL Call failed: %s. Exiting' % e)
            return None, None, None, {'flag': -1}

        if error_type == 'normed_error':
            error = _error / model if model != 0 else -1
        elif error_type == 'median_avg_error':
            error = data[-1] - seasonal - median(data)
        elif error_type == 'stl_error':
            error = _error

        # add error to distribution and evaluate
        self.td.add(error, 1.0)
        state = self.error_eval[self.params['error_handling']](error)

        return seasonal, trend, error, state

    def write(self, state):
        seasonal, trend, error, state = state
        # store distribution
        self.metric_store.write(self.tdigest_key, self.td.serialize())
        # write states
        prefix = '%s.%s' % (self.plugin, self.service)
        for name, value in state.iteritems():
            metric_name = '%s.%s' % (prefix, name)
            self.sink.write(metric_name, value)
        self.sink.write('%s.%s' % (prefix, 'seasonal'), seasonal)
        self.sink.write('%s.%s' % (prefix, 'trend'), trend)
        self.sink.write('%s.%s' % (prefix, 'error'), error)

    def run(self):
        data = self.read()
        if data:
            state = self.process(data)
            self.write(state)
        else:
            return None

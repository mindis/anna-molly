import re
import sys
import unittest

sys.path.append("../")

from mock import Mock
from sure import expect

from fixtures.config import CONFIG
from lib.plugins import seasonal_decomposition
from lib.modules.models import TimeSeriesTuple

# TODO
# mock GraphiteSink
# test for run
# test for eval methods


class TestSeasonalDecomposition(unittest.TestCase):

    def stub_read(self, metric):
        data = []
        for key, value in self.meta_store.iteritems():
            if re.match(metric, key):
                data.append(value)
        return data

    def stub_write(self, metric, val):
        self.write_pipeline[metric] = val

    def stub_quantile(self, quantile):
        return quantile

    def setUp(self):
        self.meta_store = {
            'service1:1100': TimeSeriesTuple('service1', 1100, 0.1),
            'service1:1110': TimeSeriesTuple('service1', 1110, 0.5),
            'service1:1130': TimeSeriesTuple('service1', 1130, 1.0),
            'service1:1140': TimeSeriesTuple('service1', 1140, 2.0),
            'service1:1170': TimeSeriesTuple('service1', 1170, 1.0),
            'service1:1180': TimeSeriesTuple('service1', 1180, 1.0),
            'service1:1190': TimeSeriesTuple('service1', 1190, 1.0),
        }
        self.options = {
            'plugin': 'SeasonalDecomposition',
            'service': 'service',
            'params': {
                "metric": "service1:.*",
                "period_length": 3,
                "seasons": 3,
                "error_type": "norm",
                "default": 0,
                "error_handling": "tukey",
                "error_params": {
                    "iqr_scaling": 2,
                    "min_th_up": 0.1,
                    "max_th_up": 0.9,
                    "min_th_low": -0.1,
                    "max_th_low": -1
                }
            }
        }
        seasonal_decomposition.time = Mock(return_value=1130)
        self.test_seasonal_decomposition = seasonal_decomposition.SeasonalDecomposition(
            config=CONFIG, options=self.options)
        self.test_seasonal_decomposition.logger = Mock()
        self.test_seasonal_decomposition.TDigest = Mock()
        self.test_seasonal_decomposition.TDigest.quantile = self.stub_quantile
        self.test_seasonal_decomposition.robjects = Mock()

        self.test_seasonal_decomposition.metric_store.read = self.stub_read
        self.test_seasonal_decomposition.metric_store.write = Mock()
        self.write_pipeline = {}
        self.test_seasonal_decomposition.sink.write = self.stub_write

    def tearDown(self):
        self.test_seasonal_decomposition = None

    def test_seasonal_decomposition_should_be_callable(self):
        self.test_seasonal_decomposition.should.be.a(
            seasonal_decomposition.SeasonalDecomposition)
        self.test_seasonal_decomposition.should.have.property(
            'plugin').being.equal('SeasonalDecomposition')
        self.test_seasonal_decomposition.should.have.property(
            'service').being.equal(self.options['service'])
        self.test_seasonal_decomposition.should.have.property(
            'params').being.equal(self.options['params'])

    def test_read_for_valid_data(self):
        self.meta_store['service1:1120'] = TimeSeriesTuple(
            'service1', 1120, 0.0)
        self.meta_store['service1:1150'] = TimeSeriesTuple(
            'service1', 1150, 0.0)
        self.meta_store['service1:1160'] = TimeSeriesTuple(
            'service1', 1160, 0.0)
        data = self.test_seasonal_decomposition.read()
        exp = [TimeSeriesTuple('service1', 1100, 0.1),
               TimeSeriesTuple('service1', 1110, 0.5),
               TimeSeriesTuple('service1', 1120, 0.0),
               TimeSeriesTuple('service1', 1130, 1.0),
               TimeSeriesTuple('service1', 1140, 2.0),
               TimeSeriesTuple('service1', 1150, 0.0),
               TimeSeriesTuple('service1', 1160, 0.0),
               TimeSeriesTuple('service1', 1170, 1.0),
               TimeSeriesTuple('service1', 1180, 1.0)]
        expect(data).to.be.equal(exp)

    def test_read_for_valid_data_but_missing_datapoints(self):
        data = self.test_seasonal_decomposition.read()
        exp = [TimeSeriesTuple('service1', 1100, 0.1),
               TimeSeriesTuple('service1', 1110, 0.5),
               TimeSeriesTuple('service1', 1120, 0.0),
               TimeSeriesTuple('service1', 1130, 1.0),
               TimeSeriesTuple('service1', 1140, 2.0),
               TimeSeriesTuple('service1', 1150, 0.0),
               TimeSeriesTuple('service1', 1160, 0.0),
               TimeSeriesTuple('service1', 1170, 1.0),
               TimeSeriesTuple('service1', 1180, 1.0)]
        expect(data).to.be.equal(exp)

    def test_read_for_invalid_data(self):
        self.meta_store = {
            'service1:1100': TimeSeriesTuple('service1', 1100, 0.1),
            'service1:1110': TimeSeriesTuple('service1', 1110, 0.5),
            'service1:1130': TimeSeriesTuple('service1', 1130, 1.0),
            'service1:1140': TimeSeriesTuple('service1', 1140, 2.0)
        }
        data = self.test_seasonal_decomposition.read()
        expect(data).to.be.equal(None)

    def test_write(self):
        self.test_seasonal_decomposition.write(
            (1.0, 2.0, 10, {'flag': 1, 'upper': 5, 'lower': -5}))
        # TO DO
        # assert write tdigest call
        prefix = 'SeasonalDecomposition.service.'
        expect(self.write_pipeline).to.be.equal({prefix + 'seasonal': 1.0,
                                                 prefix + 'trend': 2.0,
                                                 prefix + 'error': 10,
                                                 prefix + 'flag': 1,
                                                 prefix + 'upper': 5,
                                                 prefix + 'lower': -5})

    def test_eval_quantile(self):
        pass

    def test_eval_tukey(self):
        pass
        # exp = {'upper': 0.0, 'flag': 1, 'lower': 0.0}
        # self.test_seasonal_decomposition._eval_tukey(0.5)

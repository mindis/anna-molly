import numpy as np
import re
import sys
import unittest

sys.path.append("../")

from mock import Mock
from struct import pack
from sure import expect

from fixtures.config import CONFIG
from lib.plugins import seasonal_decomposition
from lib.modules.models import TimeSeriesTuple

# TODO
# mock robjects


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
                "error_type": "stl",
                "default": 0,
                "error_handling": "tukey",
                "error_params": {
                    "iqr_scaling": 1.5,
                }
            }
        }
        seasonal_decomposition.time = Mock(return_value=1130)
        # seasonal_decomposition.robjects = Mock()
        self.test_seasonal_decomposition = seasonal_decomposition.SeasonalDecomposition(
            config=CONFIG, options=self.options)
        self.test_seasonal_decomposition.logger = Mock()
        self.test_seasonal_decomposition.robjects = Mock()
        self.test_seasonal_decomposition.td.quantile = self.stub_quantile
        self.test_seasonal_decomposition.metric_store.read = self.stub_read
        self.test_seasonal_decomposition.metric_store.write = Mock()
        self.test_seasonal_decomposition.graphite_sink = Mock()
        self.write_pipeline = {}
        self.test_seasonal_decomposition.graphite_sink.write = self.stub_write

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

    def test_eval_quantile(self):
        self.test_seasonal_decomposition = None
        self.options['params']['error_handling'] = 'quantile'
        self.options['params']['error_params']['alpha'] = 0.10
        self.test_seasonal_decomposition = seasonal_decomposition.SeasonalDecomposition(
            config=CONFIG, options=self.options)
        self.test_seasonal_decomposition.td.quantile = self.stub_quantile
        # no outlier
        error = 0.9
        res = self.test_seasonal_decomposition._eval_quantile(error)
        expect(res).to.be.equal({'flag': 0,
                                 'alpha': 0.1,
                                 'lower': 0.05,
                                 'upper': 0.95})
        # outlier
        error = 0.99
        res = self.test_seasonal_decomposition._eval_quantile(error)
        expect(res).to.be.equal({'flag': 1,
                                 'alpha': 0.1,
                                 'lower': 0.05,
                                 'upper': 0.95})
        # no outlier using minimal thresholds
        self.options['params']['error_params'][
            'minimal_upper_threshold'] = 0.94
        error = 0.9
        res = self.test_seasonal_decomposition._eval_quantile(error)
        expect(res).to.be.equal({'flag': 0,
                                 'alpha': 0.1,
                                 'lower': 0.05,
                                 'upper': 0.94})
        # outlier using minimal thresholds
        self.options['params']['error_params'][
            'minimal_upper_threshold'] = 0.96
        error = 0.955
        res = self.test_seasonal_decomposition._eval_quantile(error)
        expect(res).to.be.equal({'flag': 1,
                                 'alpha': 0.1,
                                 'lower': 0.05,
                                 'upper': 0.95})

    def test_eval_tukey(self):
        # no outlier
        error = 0.5
        res = self.test_seasonal_decomposition._eval_tukey(error)
        expect(res).to.be.equal({'flag': 0,
                                 'lower': -0.5,
                                 'upper': 1.5})
        # outlier
        error = 2
        res = self.test_seasonal_decomposition._eval_tukey(error)
        expect(res).to.be.equal({'flag': 1,
                                 'lower': -0.5,
                                 'upper': 1.5})
        # no outlier using minimal thresholds
        self.options['params']['error_params'][
            'minimal_lower_threshold'] = -0.4
        error = -0.3
        res = self.test_seasonal_decomposition._eval_tukey(error)
        expect(res).to.be.equal({'flag': 0,
                                 'lower': -0.4,
                                 'upper': 1.5})
        # outlier using minimal thresholds
        self.options['params']['error_params'][
            'minimal_lower_threshold'] = -0.4
        error = -0.41
        res = self.test_seasonal_decomposition._eval_tukey(error)
        expect(res).to.be.equal({'flag': -1,
                                 'lower': -0.4,
                                 'upper': 1.5})

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

    # def test_process_with_inproper_stl_call(self):
    #     self.test_seasonal_decomposition.robjects.r.stl = Mock(return_value=[])
    #     self.test_seasonal_decomposition.robjects.r.ts = Mock(return_value=[])
    # data = self.test_seasonal_decomposition.read()
    #     data = []
    #     expect(self.test_seasonal_decomposition.process(data)).to.be.equal((None, None, None, {'flag': -1}))

    # def test_process_with_proper_stl_call(self):
    #     data = [TimeSeriesTuple('service1', 100, 6.1),
    #             TimeSeriesTuple('service1', 110, 7.1),
    #             TimeSeriesTuple('service1', 120, 8.1)]
    #     array = [[[1,2,3], [5,5,5], [0.1,0.1,0.1]], None]
    # normalized error
    # TODO
    # NEEDS TO BE MOCKED
    # self.test_seasonal_decomposition.robjects.r.ts = Mock()
    # self.test_seasonal_decomposition.robjects.r.stl = Mock(return_value=array)
    #     self.test_seasonal_decomposition._eval_tukey = Mock(return_value={})
    #     self.test_seasonal_decomposition.td.add = Mock()
    #     res = self.test_seasonal_decomposition.process(data)
    #     self.test_seasonal_decomposition.td.add.assert_called_with(0.0125, 1.0)
    #     expect(res).to.be.equal((3, 5, 0.0125, {}))
    # median avg error
    #     self.options['params']['error_type'] = 'median'
    #     self.test_seasonal_decomposition = None
    #     self.test_seasonal_decomposition = seasonal_decomposition.SeasonalDecomposition(config=CONFIG, options=self.options)
    # TODO
    # NEEDS TO BE MOCKED
    # self.test_seasonal_decomposition.robjects.r.ts = Mock()
    # self.test_seasonal_decomposition.robjects.r.stl = Mock(return_value=array)
    #     self.test_seasonal_decomposition._eval_tukey = Mock(return_value={})
    #     self.test_seasonal_decomposition.td.add = Mock()
    #     res = self.test_seasonal_decomposition.process(data)
    #     self.test_seasonal_decomposition.td.add.assert_called_with(0.1, 1.0)
    #     expect(res).to.be.equal((3, 5, -2, {}))
    # stl error
    #     self.options['params']['error_type'] = 'stl'
    #     self.test_seasonal_decomposition = None
    #     self.test_seasonal_decomposition = seasonal_decomposition.SeasonalDecomposition(config=CONFIG, options=self.options)
    # TODO
    # NEEDS TO BE MOCKED
    # self.test_seasonal_decomposition.robjects.r.ts = Mock()
    # self.test_seasonal_decomposition.robjects.r.stl = Mock(return_value=array)
    #     self.test_seasonal_decomposition._eval_tukey = Mock(return_value={})
    #     self.test_seasonal_decomposition.td.add = Mock()
    #     res = self.test_seasonal_decomposition.process(data)
    #     self.test_seasonal_decomposition.td.add.assert_called_with(0.1, 1.0)
    #     expect(res).to.be.equal((3, 5, 0.1, {}))

    def test_write(self):
        self.test_seasonal_decomposition.metric_store.write = Mock()
        self.test_seasonal_decomposition.write(
            (1.0, 2.0, 10, {'flag': 1, 'upper': 5, 'lower': -5}))
        assert self.test_seasonal_decomposition.metric_store.write.called
        prefix = 'SeasonalDecomposition.service.'
        expect(self.write_pipeline).to.be.equal({prefix + 'seasonal': 1.0,
                                                 prefix + 'trend': 2.0,
                                                 prefix + 'error': 10,
                                                 prefix + 'flag': 1,
                                                 prefix + 'upper': 5,
                                                 prefix + 'lower': -5})

    def test_run_valid_data(self):
        self.test_seasonal_decomposition.read = Mock(return_value='data')
        self.test_seasonal_decomposition.process = Mock(return_value='state')
        self.test_seasonal_decomposition.write = Mock(return_value=True)
        self.test_seasonal_decomposition.run()
        self.test_seasonal_decomposition.read.assert_called_with()
        self.test_seasonal_decomposition.process.assert_called_with('data')
        self.test_seasonal_decomposition.write.assert_called_with('state')

    def test_run_invalid_data(self):
        self.test_seasonal_decomposition.read = Mock(return_value=None)
        expect(self.test_seasonal_decomposition.run()).to.be.equal(None)

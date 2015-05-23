import pickle
import re
import sys
import unittest

sys.path.append("../")

from mock import Mock
from sure import expect

from fixtures.config import CONFIG
from lib.plugins import tukeys_filter
from lib.modules.models import TimeSeriesTuple

## TODO
# test for configured upper and lower thresholds


class TestTukeysFilter(unittest.TestCase):

    def stub_read(self, metric):
        data = []
        for key, value in self.meta_store.iteritems():
            if re.match(metric, key):
                data.append(value)
        return data

    def stub_write(self, metric, val):
        self.write_pipeline[metric] = val

    def setUp(self):
        self.meta_store = {
            'host.ip:0-0-0-1.cpu_prct_used:1100': TimeSeriesTuple('host.ip:0-0-0-1.cpu_prct_used', 1100, 1.0),
            'host.ip:0-0-0-1.cpu_prct_used:1110': TimeSeriesTuple('host.ip:0-0-0-1.cpu_prct_used', 1110, 0.0),
            'host.ip:0-0-0-1.cpu_prct_used:1120': TimeSeriesTuple('host.ip:0-0-0-1.cpu_prct_used', 1120, 2.0),
            'host.ip:0-0-0-1.cpu_prct_used:1130': TimeSeriesTuple('host.ip:0-0-0-1.cpu_prct_used', 1130, 100.0),
            'host.ip:0-0-0-2.cpu_prct_used:1100': TimeSeriesTuple('host.ip:0-0-0-2.cpu_prct_used', 1140, 1.0),
            'host.ip:0-0-0-2.cpu_prct_used:1125': TimeSeriesTuple('host.ip:0-0-0-2.cpu_prct_used', 1125, 2.5),
            'host.ip:0-0-0-2.cpu_prct_used:1115': TimeSeriesTuple('host.ip:0-0-0-2.cpu_prct_used', 1115, 5.0),
            'host.ip:0-0-0-2.cpu_prct_used:1130': TimeSeriesTuple('host.ip:0-0-0-2.cpu_prct_used', 1130, 100.0),
            'service.quartil_25:1100': TimeSeriesTuple('service.quartil_25', 1100, 0.0),
            'service.quartil_25:1120': TimeSeriesTuple('service.quartil_25', 1120, 0.5),
            'service.quartil_75:1100': TimeSeriesTuple('service.quartil_75', 1100, 1.0),
            'service.quartil_75:1120': TimeSeriesTuple('service.quartil_75', 1120, 1.5),
        }
        self.options = {
            'plugin': 'TukeysFilter',
            'service': 'cpu',
            'params': {
                'quantile_25': 'service.quartil_25',
                'quantile_75': 'service.quartil_75',
                'metrics': 'host.*cpu.*',
                'iqr_scaling': 1.5,
                'delay': 10,
                'default': 0}
        }
        tukeys_filter.time = Mock(return_value=1130)
        self.test_tukeys_filter = tukeys_filter.TukeysFilter(config=CONFIG, options=self.options)
        self.test_tukeys_filter.logger = Mock()
        self.test_tukeys_filter.metric_store.read = self.stub_read
        self.write_pipeline = {}
        self.test_tukeys_filter.sink.write = self.stub_write

    def tearDown(self):
        self.test_tukeys_filter = None

    def test_tukeys_filter_should_be_callable(self):
        self.test_tukeys_filter.should.be.a(tukeys_filter.TukeysFilter)
        self.test_tukeys_filter.should.have.property('plugin').being.equal('TukeysFilter')
        self.test_tukeys_filter.should.have.property('service').being.equal(self.options['service'])
        self.test_tukeys_filter.should.have.property('params').being.equal(self.options['params'])

    def test_read_for_valid_data(self):
        quantile_25, quantile_75, distribution = self.test_tukeys_filter.read()
        expect(quantile_25).to.be.equal(0.5)
        expect(quantile_75).to.be.equal(1.5)
        expect(distribution).to.be.equal({'host.ip:0-0-0-1.cpu_prct_used': 2.0,
                                          'host.ip:0-0-0-2.cpu_prct_used': 5.0})

    def test_read_for_missing_quantile_keys(self):
        self.meta_store.pop('service.quartil_75:1100')
        self.meta_store.pop('service.quartil_75:1120')
        expect(self.test_tukeys_filter.read()).to.equal(None)

    def test_read_for_missing_distribution_values(self):
        self.meta_store = {
            'host.ip:0-0-0-1.cpu_prct_used:100': TimeSeriesTuple('host.ip:0-0-0-1.cpu_prct_used', 100, 1.0),
            'host.ip:0-0-0-1.cpu_prct_used:110': TimeSeriesTuple('host.ip:0-0-0-1.cpu_prct_used', 110, 0.0),
            'host.ip:0-0-0-1.cpu_prct_used:120': TimeSeriesTuple('host.ip:0-0-0-1.cpu_prct_used', 120, 2.0),
            'host.ip:0-0-0-1.cpu_prct_used:130': TimeSeriesTuple('host.ip:0-0-0-1.cpu_prct_used', 130, 100.0),
            'host.ip:0-0-0-2.prct_used:1100': TimeSeriesTuple('host.ip:0-0-0-2.prct_used', 1140, 1.0),
            'host.ip:0-0-0-2.prct_used:1125': TimeSeriesTuple('host.ip:0-0-0-2.prct_used', 1125, 2.5),
            'host.ip:0-0-0-2.prct_used:1115': TimeSeriesTuple('host.ip:0-0-0-2.prct_used', 1115, 5.0),
            'host.ip:0-0-0-2.prct_used:1130': TimeSeriesTuple('host.ip:0-0-0-2.prct_used', 1130, 100.0),
            'service.quartil_25:1100': TimeSeriesTuple('service.quartil_25', 1100, 0.0),
            'service.quartil_25:1120': TimeSeriesTuple('service.quartil_25', 1120, 0.5),
            'service.quartil_75:1100': TimeSeriesTuple('service.quartil_75', 1100, 1.0),
            'service.quartil_75:1120': TimeSeriesTuple('service.quartil_75', 1120, 1.5),
        }
        expect(self.test_tukeys_filter.read()).to.equal(None)

    def test_read_for_too_old_keys(self):
        self.meta_store.pop('service.quartil_75:1100')
        self.meta_store.pop('service.quartil_75:1120')
        self.meta_store['service.quartil_75:10'] = TimeSeriesTuple('service.quartil_75', 10, 1.0)
        self.meta_store['service.quartil_75:20'] = TimeSeriesTuple('service.quartil_75', 20, 1.5)
        expect(self.test_tukeys_filter.read()).to.equal(None)

    def test_read_for_inconsistent_quantile_values(self):
        self.meta_store['service.quartil_75:1110'] = TimeSeriesTuple('service.quartil_75', 1110, -1.0)
        self.meta_store['service.quartil_75:1120'] = TimeSeriesTuple('service.quartil_75', 1120, -1.0)
        expect(self.test_tukeys_filter.read()).to.equal(None)

    def test_process(self):
        data = self.test_tukeys_filter.read()
        quantile_25, quantile_75, states = self.test_tukeys_filter.process(data)
        expect(quantile_25).to.be.equal(0.5)
        expect(quantile_75).to.be.equal(1.5)
        expect(states).to.be.equal({'host.ip:0-0-0-1.cpu_prct_used': 0,
                                    'host.ip:0-0-0-2.cpu_prct_used': 1})

    def test_process_with_additional_static_thresholds(self):
        pass
        # TODO

    def test_write(self):
        data = self.test_tukeys_filter.read()
        quantile_25, quantile_75, states = self.test_tukeys_filter.process(data)
        self.test_tukeys_filter.write((quantile_25, quantile_75, states))
        prefix = 'TukeysFilter.cpu.'
        expect(self.write_pipeline).to.be.equal({prefix + 'quantile_25': quantile_25,
                                                 prefix + 'quantile_75': quantile_75,
                                                 prefix + 'count': 2,
                                                 prefix + 'invalid': 1,
                                                 prefix + '0-0-0-1': 0,
                                                 prefix + '0-0-0-2': 1})

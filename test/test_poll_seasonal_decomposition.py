import sys
import unittest

sys.path.append("../")

from mock import Mock

from fixtures.config import CONFIG
from lib.plugins import poll_seasonal_decomposition, seasonal_decomposition

## TODO
# new config structure also in poll script 

class TestPollSeasonalDecomposition(unittest.TestCase):

    def setUp(self):
        self.test_poll_seasonal_decomposition = poll_seasonal_decomposition.PollSeasonalDecomposition(config=CONFIG, options=None)

    def tearDown(self):
        self.test_poll_seasonal_decomposition = None

    def test_seasonal_decomposition_should_be_callable(self):
        self.test_poll_seasonal_decomposition.should.be.a(poll_seasonal_decomposition.PollSeasonalDecomposition)
        self.test_poll_seasonal_decomposition.should.have.property('plugin').being.equal('SeasonalDecomposition')

    def test_run_valid_input(self):
        self.test_poll_seasonal_decomposition.app.task_runner.delay = Mock()
        self.algo_config = {'cpu': {'option': 0}}
        self.test_poll_seasonal_decomposition.run()
        self.test_poll_seasonal_decomposition.app.task_runner.delay.assert_called_once_with(seasonal_decomposition.SeasonalDecomposition,
        {'options': {'option': 0}, 'plugin': 'SeasonalDecomposition', 'service': 'cpu'})

    def test_run_valid_input(self):
        self.test_poll_seasonal_decomposition.app.task_runner.delay = Mock()
        self.algo_config = {'cpu': None}
        self.test_poll_seasonal_decomposition.run()
        assert not self.test_poll_seasonal_decomposition.app.task_runner.delay

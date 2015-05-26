import sys
import unittest

sys.path.append("../")

from mock import Mock

from fixtures.config import CONFIG
from lib.plugins import poll_tukeys_filter, tukeys_filter

## TODO
# new config structure also in poll script 

class TestPollTukeysFilter(unittest.TestCase):

    def setUp(self):
        self.test_poll_tukeys_filter = poll_tukeys_filter.PollTukeysFilter(config=CONFIG, options=None)

    def tearDown(self):
        self.test_poll_tukeys_filter = None

    def test_seasonal_decomposition_should_be_callable(self):
        self.test_poll_tukeys_filter.should.be.a(poll_tukeys_filter.PollTukeysFilter)
        self.test_poll_tukeys_filter.should.have.property('plugin').being.equal('TukeysFilter')

    def test_run_valid_input(self):
        self.test_poll_tukeys_filter.app.task_runner.delay = Mock()
        self.algo_config = {'cpu': {'option': 0}}
        self.test_poll_tukeys_filter.run()
        self.test_poll_tukeys_filter.app.task_runner.delay.assert_called_once_with(tukeys_filter.TukeysFilter,
        {'options': {'option': 0}, 'plugin': 'TukeysFilter', 'service': 'cpu'})

    def test_run_valid_input(self):
        self.test_poll_tukeys_filter.app.task_runner.delay = Mock()
        self.algo_config = {'cpu': None}
        self.test_poll_tukeys_filter.run()
        assert not self.test_poll_tukeys_filter.app.task_runner.delay

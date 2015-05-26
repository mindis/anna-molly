"""
Poll Script for Tukeys Outlier Filter
"""

from .. import app
from lib.modules.base_task import BaseTask
from lib.modules import config as config_loader

from tukeys_filter import TukeysFilter


class PollTukeysFilter(BaseTask):

    def __init__(self, config, options):
        super(PollTukeysFilter, self).__init__(config, resource={'metric_store': 'RedisSink'})
        self.plugin = 'TukeysFilter'

    def run(self):
        """
        """
        algo_config = config_loader.load('/opt/anna-molly/config/services.json')
        algo_config = algo_config.get(self.plugin, {None: None})
        for service, options in algo_config.iteritems():
            if service and options:
                params = {'options': options, 'plugin': self.plugin, 'service': service}
                app.task_runner.delay(TukeysFilter, params)
        return True

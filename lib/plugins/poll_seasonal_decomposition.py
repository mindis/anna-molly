"""
Poll Script for Seasonal Decomposition
"""


from .. import app
from lib.modules.base_task import BaseTask
from lib.modules import config as config_loader

from seasonal_decomposition import SeasonalDecomposition


class PollSeasonalDecomposition(BaseTask):

    def __init__(self, config, options):
        super(PollSeasonalDecomposition, self).__init__(config, resource={'metric_store': 'RedisSink'})
        self.plugin = 'SeasonalDecomposition'

    def run(self):
        """
        """
        algo_config = config_loader.load('/opt/anna-molly/config/services.json')
        algo_config = algo_config.get(self.plugin, {None: None})
        for service, options in algo_config.iteritems():
            if service and options:
                params = {'service': service, 'options': options, 'plugin': self.plugin}
                app.task_runner.delay(SeasonalDecomposition, params)
        return True

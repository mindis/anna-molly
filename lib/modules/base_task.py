"""
Base Task
"""

import sink


class BaseTask(object):
    """
    """
    def __init__(self, config, resource={}):
        self.config = config
        self.resource = resource
        self._data_store = None
        self._output_sink = None
        self.data_store = self.resource.get('data_store', None)
        self.output_sink = self.resource.get('output_sink', None)

    @property
    def data_store(self):
        return self._data_store

    @data_store.setter
    def data_store(self, value):
        if value:
            config = self.config['data_store'][value]
            self._data_store = getattr(sink, value)(config)
        else:
            self._data_store = None

    @property
    def output_sink(self):
        return self._output_sink

    @output_sink.setter
    def output_sink(self, value):
        if value:
            config = self.config['output_sink'][value]
            self._output_sink = getattr(sink, value)(config)
        else:
            self._output_sink = None

    def run(self):
        raise NotImplementedError

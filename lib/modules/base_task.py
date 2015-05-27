import sink


class BaseTask(object):

    """
    """

    def __init__(self, config, resource={}):
        self.config = config
        self.resource = resource
        self._metric_store = None
        self._sink = None
        self.metric_store = self.resource.get('metric_store', None)
        self.graphite_sink = self.resource.get('graphite_sink', None)

    @property
    def metric_store(self):
        return self._metric_store

    @metric_store.setter
    def metric_store(self, value):
        if value:
            config = self.config['metric_store']
            self._metric_store = getattr(sink, value)(config)
        else:
            self._metric_store = None

    @property
    def sink(self):
        return self.graphite_sink

    @sink.setter
    def sink(self, value):
        if value:
            config = self.config['graphite_sink']
            print value
            self.graphite_sink = getattr(sink, value)(config)
        else:
            self.graphite_sink = None

    def run(self):
        raise NotImplementedError

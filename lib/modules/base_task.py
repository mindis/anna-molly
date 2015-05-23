import sink

# TODO. Needs improvement. Rename sink/metric_store


class BaseTask(object):

    """
    """

    def __init__(self, config, resource={}):
        self.config = config
        self.resource = resource
        self._metric_store = None
        self._sink = None
        self.metric_store = self.resource.get('metric_store', None)
        # FIXME: needs rename
        self.sink = self.resource.get('sink', None)

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
        return self._sink

    @sink.setter
    def sink(self, value):
        if value:
            config = self.config['sink']
            self._sink = getattr(sink, value)(config)
        else:
            self._sink = None

    def run(self):
        raise NotImplementedError

from collections import namedtuple


class TimeSeriesTuple(namedtuple('TimeSeriesTuple', 'name timestamp value')):

    __slots__ = ()

    def __str__(self):
        return "TimeSeriesTuple: name=%s timestamp=%d value=%f" % (self.name, self.timestamp, float(self.value))

    def transform(self):
        return self


class TSV(object):

    def __init__(self, value):
        self.name = value.name
        self.timestamp = value.timestamp
        self.value = float(value.value)

    def __str__(self):
        return "%s\t%d\t%f" % (self.name, self.timestamp, self.value)

    def transform(self):
        return TimeSeriesTuple(self.name, self.timestamp, self.value)


class CSV(TSV):

    def __str__(self):
        return "%s,%d,%f" % (self.name, self.timestamp, self.value)


class RedisLastValue(object):

    def __init__(self, defaults, value):
        defaults = defaults["defaults"]
        self.ttl = defaults["ttl"]
        self.value = float(value.value)
        self.timestamp = value.timestamp
        self.name = self.build_name(value)

    def __str__(self):
        return "%s,%s,%s with TTL: %s" % (self.name, self.timestamp, self.value, self.ttl)

    def build_name(self, value):
        return value.name


class RedisTimeStamped(RedisLastValue):

    def build_name(self, value):
        return '%s:%s' % (value.name, value.timestamp)


class AerospikeTimeStamped(object):

    def __init__(self, defaults, datapoint):
        defaults = defaults["defaults"]
        self.ttl = defaults["ttl"]
        self.namespace = defaults["namespace"]
        self.set = defaults["set"]
        self.name = datapoint.name
        self.key = (self.namespace, self.set, self.name)
        self.bin = {
            "timestamp": datapoint.timestamp,
            "value": datapoint.value
        }

    def __str__(self):
        return "%s,%s,%s with TTL: %s stored at\n Namespace:%s\n Set:%s\n" % (
            self.name, self.bin['timestamp'], self.bin['value'],
            self.ttl, self.namespace, self.set
        )

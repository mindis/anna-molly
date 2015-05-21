from collections import namedtuple


class TimeSeriesTuple(namedtuple('TimeSeriesTuple', 'name timestamp value')):

    __slots__ = ()

    def __str__(self):
        return "TimeSeriesTuple: name=%s timestamp=%d value=%f" % (self.name, self.timestamp, float(self.value))

    def transform(self):
        return self


class RedisLastValue(object):

    def __init__(self, defaults, value):
        # FIXME
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

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

    def __init__(self, value, ttl):
        self.ttl = ttl
        self.value = float(value.value)
        self.timestamp = value.timestamp
        self.name = self.build_name(value)

    def __str__(self):
        return "%s,%d,%f with TTL: %d" % (self.name, self.timestamp, self.value, self.ttl)

    def build_name(self, value):
        return value.name


class RedisTimestamped(RedisLastValue):

    def build_name(self, value):
        return '%s:%s' % (value.name, value.timestamp)

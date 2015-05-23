import redis
import socket
import cPickle as pickle
from cStringIO import StringIO

from twitter.common.lang import Interface
from twitter.common import log


class Sink(Interface):

    def __init__(self):
        pass

    def connect(self):
        pass

    def write(self):
        pass

    def close(self):
        pass

    def read(self):
        pass


class RedisSink(Sink):

    def __init__(self, config):
        self.host = config['host']
        self.port = config['port']
        self.count = 0
        self.pipeline_size = config.get('pipeline_size', None) or 100
        self.connection = self.connect()

    def connect(self):
        try:
            redis_conn = redis.StrictRedis(host=self.host, port=self.port)
            self.redis_pipeline = redis_conn.pipeline()
            return redis_conn
        except Exception as _e:
            log.error("RedisSink: ConnectionError\n %s %s" % (config, str(_e)))

    def write(self, datapoints):
        for datapoint in datapoints:
            self.count += 1
            self.redis_pipeline.setex(
                datapoint.name,
                datapoint.ttl,
                pickle.dumps(datapoint.datapoint)
            )
            if self.count % self.pipeline_size == 0:
                self.redis_pipeline.execute()

    def read_keys(self, pattern):
        for item in self.connection.scan_iter(match=pattern):
            yield item

    def read(self, pattern):
        for item in self.connection.scan_iter(match=pattern):
            yield pickle.Unpickler(StringIO(self.connection.get(item))).load()


class GraphiteSink(Sink):

    def __init__(self, config):
        self.host = config['host']
        self.port = config['port']
        self.prefix = config['prefix']
        self.connection = self.connect()

    def connect(self):
        try:
            sock = socket.socket()
            sock.connect((self.host, self.port))
            return sock
        except Exception as _e:
            log.error("Cannot connect to Graphite Sink with config:%s\n%s" %(config, str(_e)))

    def write(self, datapoints):
        for datapoint in datapoints:
            try:
                self.connection.sendall("%s.%f %s %d\n" % (datapoint.name, datapoint.value, data.timestamp))
            except Exception as _e:
                log.error("GraphiteSink: WriteError\n %s \n%s" % (datapoint, str(_e)))

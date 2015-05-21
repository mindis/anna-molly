import redis
import rediscluster
import cPickle as pickle
from cStringIO import StringIO

# This can surely be improved
from models.TimeSeriesTuple import toTimeSeriesTuple

from twitter.common.lang import Interface


class Sink(Interface):

    def __init__(self):
        pass

    def connect(self):
        pass

    def write(self):
        pass

    def close(self):
        pass


class RedisSink(Sink):

    def __init__(self, config):
        self.host = config['host']
        self.port = config['port']
        self.count = 0
        self.pipeline_size = config.get('pipeline_size', None) or 100
        self.connection = self.connect()

    def connect(self):
        redis_conn = redis.StrictRedis(host=self.host, port=self.port)
        self.redis_pipeline = redis_conn.pipeline()
        return redis_conn

    def write(self, datapoint):
        self.count += 1
        self.redis_pipeline.setex(
            datapoint.name, datapoint.ttl, pickle.dumps(datapoint))
        if self.count % self.pipeline_size == 0:
            self.redis_pipeline.execute()

    def read_keys(self, pattern):
        for item in self.connection.scan_iter(match=pattern):
            yield item

    def read(self, pattern):
        for item in self.connection.scan_iter(match=pattern):
            yield pickle.Unpickler(StringIO(self.connection.get(item))).load().toTimeSeriesTuple()


class RedisClusterSink(RedisSink):

    def connect(self):
        startup_nodes = [{
            'host': self.host,
            'port': self.port
        }]
        self.redis_cluster = rediscluster.RedisCluster(startup_nodes=startup_nodes,
                                                       decode_responses=True)
        self.redis_pipeline = self.redis_cluster.pipeline()

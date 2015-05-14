import redis
import rediscluster
import aerospike
from models import AerospikeTimeStamped

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
        self.redis_conn = redis.StrictRedis(host=self.host, port=self.port)
        self.redis_pipeline = self.redis_conn.pipeline()

    def write(self, datapoint):
        self.count += 1
        self.redis_pipeline.setex(datapoint.name, datapoint.ttl, datapoint.value)
        if self.count % self.pipeline_size == 0:
            self.redis_pipeline.execute()


class RedisClusterSink(RedisSink):

    def connect(self):
        startup_nodes = [{
            'host': self.host,
            'port': self.port
        }]
        self.redis_cluster = rediscluster.RedisCluster(startup_nodes=startup_nodes,
                                                       decode_responses=True)
        self.redis_pipeline = self.redis_cluster.pipeline()


class AerospikeSink(Sink):

    def __init__(self, config):
        self.connection = aerospike.client(config)

    def connect(self):
        self.connection.connect()

    def write(self, datapoint):
        if type(datapoint) == AerospikeTimeStamped:
            self.connection.put(datapoint.key, datapoint.bin)

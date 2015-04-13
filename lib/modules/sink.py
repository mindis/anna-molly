import redis
import rediscluster

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
        self.pipeline_size = config['pipeline_size']
        self.connection = self.connect()

    def connect(self):
        self.redis_conn = redis.StrictRedis(host=self.host, port=self.port)
        self.redis_pipeline = self.redis_conn.pipeline()

    def write(self, data):
        count = 0
        for item in data:
            count += 1
            self.redis_pipeline.setex(item.name, item.ttl, item.value)
            if count % self.pipeline_size == 0:
                self.redis_pipeline.execute()


class RedisClusterSink(RedisSink):

    def connect(self):
        startup_nodes = [{
            "host": self.host,
            "port": self.port
        }]
        self.redis_cluster = rediscluster.RedisCluster(startup_nodes=startup_nodes,
                                                       decode_responses=True)
        self.redis_pipeline = self.redis_cluster.pipeline()

import csv
# TO DO: 
# import only if redis is chosen? 
import redis

from twitter.common.lang import Interface

class Sink(Interface):
    def __init__(self): pass
    def connect(self): pass
    def write(self): pass
    def read(self): pass
    def close(self): pass


class FileSink(Sink):
    def __init__(self, config):
        self.path = config['path']
        self.connection = self.connect()

    def connect(self):
        self.fp = open(self.path, "w")

    def write(self, data):
        for item in data:
            item = str(item) + "\n"
            self.fp.write(item)

    def close(self):
        self.fp.close()


class TsvSink(FileSink):
    def connect(self):
        self.fp = csv.writer(open(self.path))

    def write(self, data):
        for item in data:
            self.connection.writerows(data)


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

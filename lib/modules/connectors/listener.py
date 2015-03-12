import logging
import re
import redis
import socket
from struct import Struct, unpack

from unpickler import SafeUnpickler


class Listen(object):
    """
    The Listener listens on TCP port to receive carbon pickles and stores relevant metrics to Redis.
    """
    def __init__(self, config):
        self.config = config['LISTENER']
        self.host = self.config['CARBON']['HOST']
        self.port = self.config['CARBON']['PORT']
        self.redis_conn = redis.StrictRedis(host=self.config['REDIS']['HOST'], port=self.config['REDIS']['PORT'])
        self.redis_pipeline = self.redis_conn.pipeline()
        self.unpickler = SafeUnpickler
        self.listener = self.get_connection()

    def get_connection(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.host, self.port))
        listener.setblocking(1)
        listener.listen(5)
        return listener

    def unpickle(self, infile):
        """
        Generate a pickle from a stream
        """
        try:
            bunch = self.unpickler.loads(infile)
            yield bunch
        except EOFError:
            return

    def read_all(self, conn, length):
        """
        Read N bytes from a stream
        """
        data = ''
        while length > 0:
            buf = conn.recv(length)
            length -= len(buf)
            data += buf
        return data

    def blacklist(self, metric, patterns):
        """
        Throw metrics away, which match blacklist
        """
        for pattern in patterns:
            if re.match(pattern, metric[0]):
                return True
        return

    def build_redis_key(self, metric, key, mapping):
        KEY_BUILDER = {
            "CCM": ("%s:%d" % (metric[0], int(metric[1][0])), mapping['TTL'], metric[1][1]),
            "OUTLIER_DETECTION": (metric[0], mapping['TTL'], metric[1][1])
        }

        for pattern in mapping['WHITELIST']:
            if re.match(pattern, metric[0]):
                return KEY_BUILDER[key]
        return (None, None, None)

    def listen_and_store(self):
        """
        Listen for pickles over TCP and store relevant metrics to Redis.
        """
        conn, _ =  self.listener.accept()
        while 1:
            try:
                length = Struct('!I').unpack(self.read_all(conn, 4))
                body = self.read_all(conn, length[0])
                bunch = self.unpickle(body):
                    for metric in bunch:
                        if self.blacklist(metric, self.config['METRICS']['REDIS_BLACKLIST']):
                            continue
                        else:
                            for key, mapping in self.config['METRICS']['REDIS_MAPPING'].iteritems():
                                metric_name, ttl, metric_value = self.build_redis_key(metric, key, mapping)
                                if metric_name:
                                    self.redis_pipeline.setex(metric_name, ttl, metric_value)
                self.redis_pipeline.execute()
            except Exception as e:
                print e
                break

    def run(self):
        self.listen_and_store()

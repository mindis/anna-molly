import pyuv
import signal

from models import TimeSeriesTuple
from collections import deque

from twitter.common.lang import Interface


class Spout(Interface):

    def __init__(self):
        pass

    def listen(self):
        pass

    def stream(self):
        pass


class CarbonTcpSpout(Spout):
    def __init__(self, config, callback):
        self.host = config['spout']['carbon']['host']
        self.port = config['spout']['carbon']['port']
        self.callback = callback
        self.loop = pyuv.Loop.default_loop()
        self.server = pyuv.TCP(self.loop)
        self.clients = []
        self.signal_handler = pyuv.Signal(self.loop)

    def signal_cb(self, handle, signum):
        [client.close() for client in self.clients]
        self.signal_handler.close()
        self.server.close()

    def on_connection(self, server, error):
        client = pyuv.TCP(self.server.loop)
        self.server.accept(client)
        self.clients.append(client)
        client.start_read(self.receiver)

    def receiver(self, client, data, error):
        if data is None:
            client.close()
            self.clients.remove(client)
            return
        data = data.rstrip()
        data = data.split("\n")
        for datapoint in data:
            datapoint = datapoint.split(" ")
            self.callback(TimeSeriesTuple(datapoint[0], datapoint[1], datapoint[2]))

    def connect(self):
        try:
            self.server.bind((self.host, self.port))
            self.server.listen(self.on_connection)
            self.signal_handler.start(self.signal_cb, signal.SIGINT)
            self.loop.run()
        except Exception as _e:
            print str(_e)

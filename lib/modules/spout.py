import socket
from struct import Struct
from twitter.common.lang import Interface
from helper import SafeUnpickler
from models import TimeSeriesTuple


class Spout(Interface):
    def __init__(self):
        pass

    def connect(self):
        pass

    def stream(self):
        pass


class CarbonSyncTcpSpout(Spout):
    def __init__(self, config):
        self.host = config['spout']['carbon']['host']
        self.port = config['spout']['carbon']['port']
        self.model = config['spout']['carbon']['model']
        self.connection, _ = self.connect().accept()
        self.receive = {
            'pickle': self.receive_pickle
        }

    def read_all_pickle(self, length):
        data = ''
        while length > 0:
            buf = self.connection.recv(length)
            length -= len(buf)
            data += buf
        return data

    def receive_pickle(self):
        length = Struct('!I').unpack(self.read_all_pickle(4))
        data = self.read_all_pickle(length[0])
        return SafeUnpickler.transform(data)

    def connect(self):
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        connection.bind((self.host, self.port))
        connection.setblocking(1)
        connection.listen(5)
        return connection

    def stream(self):
        while True:
            for datapoint in self.receive[self.model]():
                yield TimeSeriesTuple(datapoint[0], datapoint[1][0],
                                      datapoint[1][1])



class CarbonAsyncTcpSpout(Spout):
    def __init__(self, config, callback):
        self.host = config['spout']['carbon']['host']
        self.port = config['spout']['carbon']['port']
        self.callback = callback
        self.clients = []
        self.loop = pyuv.Loop.default_loop()
        self.server = pyuv.TCP(self.loop)

        self.signal_handler = pyuv.Signal(self.loop)

    def signal_cb(self, handle, signum):
        [client.close() for client in self.clients]
        self.signal_handler.close()
        self.server.close()

    def on_connection(self, server, error):
        client = pyuv.TCP(self.server.loop)
        self.server.accept(client)
        self.clients.append(client)
        client.start_read(self.stream)

    def stream(self, client, data, error):
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

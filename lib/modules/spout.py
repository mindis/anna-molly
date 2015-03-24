import socket
from struct import Struct, unpack
from twitter.common.lang import Interface
from helper import SafeUnpickler
from models import TimeSeriesTuple

class Spout(Interface):
    def __init__(self): pass
    def connect(self): pass
    def stream(self): pass

class CarbonTcpSpout(Spout):
    def __init__(self, config):
        self.host = config['host']
        self.port = config['port']
        self.model = config['model']
        self.connection, _ = self.connect().accept()
        self.receive = {
            'pickle': self.receive_pickle,  
            'text': self.receive_text
        }
    def _read_all_pickle(self, length):
        data = ''
        while length > 0:
            buf = self.connection.recv(length)
            length -= len(buf)
            data += buf
        return data

    def _read_all_text(self, length):
        data = ''
        while length > 0:
            buf = self.connection.recv(length)
            length -= len(buf)
            data += buf
            if "\n" in data:
                return data
        return data

    def receive_pickle(self):
        length = Struct('!I').unpack(self._read_all_pickle(4))
        data = self._read_all_pickle(length[0])
        return SafeUnpickler.transform(data)

    def receive_text(self):
        return _read_all_text()

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
                yield TimeSeriesTuple(datapoint[0], datapoint[1][0], datapoint[1][1])


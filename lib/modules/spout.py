import signal
import socket
import struct

import pyuv
from twitter.common.lang import Interface
from twitter.common import log

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
        self.host = config['host']
        self.port = config['port']
        self.connection, _ = self.connect().accept()
        self.receive = self.receive_pickle

    def read_all_pickle(self, length):
        data = ''
        while length > 0:
            buf = self.connection.recv(length)
            length -= len(buf)
            data += buf
        return data

    def receive_pickle(self):
        try:
            length = struct.Struct('!I').unpack(self.read_all_pickle(4))
            data = self.read_all_pickle(length[0])
            return SafeUnpickler.loads(data)
        except Exception as _e:
            log.error("%s Data:%s" % (str(_e), data))

    def connect(self):
        try:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            conn.bind((self.host, self.port))
            conn.setblocking(1)
            conn.listen(5)
            log.debug("Accepting data @ Host: %s Port:%s" %
                      self.host, self.port)
            log.debug("Connection: %s" % (conn))
            return conn
        except Exception as _e:
            log.error("%s\n" % (str(_e)))

    def stream(self):
        log.debug("Streaming Metrics")
        while True:
            for datapoint in self.receive():
                log.debug("%s" % (datapoint))
                yield TimeSeriesTuple(datapoint[0],
                                      datapoint[1][0],
                                      datapoint[1][1]
                                      )


class CarbonAsyncTcpSpout(Spout):

    def __init__(self, config, callback):
        self.buf = None
        self.host = config['host']
        self.port = config['port']
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

    def unpickle(self, infile):
        try:
            bunch = SafeUnpickler.loads(infile)
            yield bunch
        except Exception as _e:
            log.error("UnpiclingError: %s" % (str(_e)))

    def stream(self, client, data, error):
        if error:
            log.error("%s" % (error))
        if data is None:
            log.debug("Closing Client %s" % (client))
            client.close()
            self.clients.remove(client)
            return
        if self.buf:
            data = self.buf + data
            self.buf = None
        # Compute Size
        size = data[0:4]
        size = struct.unpack('!I', size)[0]
        log.debug("Read Size: %s\t Received Size: %s" % (size, len(data)))
        # All okay. Read == Received. => Pickel in one packet.
        if size == (len(data) - 4):
            _data = data[4:]
        # Read < Received. => multiple pickles in packet
        elif size < (len(data) - 4):
            # Get one pickle
            _data = data[4:size + 5]
            # Stream rest again. Repeat
            self.stream(None, data[size + 4:], None)
        # Read > Recieved. => Pickle in consecutive packets.
        elif size > (len(data) - 4):
            _data = None
            # Buffer Data
            self.buf = data[size + 4:]

        for datapoints in self.unpickle(_data):
            for datapoint in datapoints:
                self.callback(TimeSeriesTuple(datapoint[0],
                                              datapoint[1][0],
                                              datapoint[1][1]))

    def connect(self):
        try:
            log.debug("Trying to connect to %s:%s" % (self.host, self.port))
            self.server.bind((self.host, self.port))
            self.server.listen(self.on_connection)
            self.signal_handler.start(self.signal_cb, signal.SIGINT)
            self.loop.run()
            log.debug("Connected")
        except Exception as _e:
            log.error("Could not connect to %s:%s %s" %
                      (self.host, self.port, str(_e)))

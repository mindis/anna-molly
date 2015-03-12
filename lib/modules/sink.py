import csv

from twitter.common.lang import Interface

class Sink(Interface):
    def __init__(self): pass
    def connect(self): pass
    def write(self): pass
    def read(self): pass
    def close(self): pass

class FileSink(Sink):
    def __init__(self, config):
        self.path = config['file']['path']
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


import json

from random import shuffle

from .tdigestcore import TDigestCore


class TDigest(object):
    def __init__(self, delta=0.1, compression=20):
        self.delta = float(delta)
        self.compression = compression
        self.tdc = TDigestCore(self.delta)

    def add(self, x, w):
        self.tdc.add(x, w)
        if len(self) > self.compression / self.delta:
            self.compress()

    def compress(self):
        auxTdc = TDigestCore(self.delta)
        centroidList = self.tdc.centroidList
        shuffle(centroidList)
        for c in centroidList:
            auxTdc.add(c.mean, c.count)
        self.tdc = auxTdc

    def quantile(self, x):
        return self.tdc.quantile(x)

    def serialize(self):
        tdobj = {}
        tdobj['centroids'] = [[c.mean, c.count] for c in self.tdc.centroidList]
        return json.dumps(tdobj)

    def __len__(self):
        return len(self.tdc)

    def __repr__(self):
        return str(self.tdc)

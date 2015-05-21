from __future__ import division
from random import shuffle

from .centroid import Centroid


class TDigestCore(object):
    def __init__(self, delta):
        self.delta = delta
        self.centroidList = []
        self.n = 1
        self.id_counter = 0

    def add(self, x, w):
        if self.centroidList:
            S = self._closest_centroids(x)
            shuffle(S)
            for c in S:

                if w == 0:
                    break
                q = self._centroid_quantile(c)
                delta_w = min(4 * self.n * self.delta * q * (1 - q) - c.count, w)
                c.add(x, delta_w)
                w -= delta_w

        if w > 0:
            self.centroidList.append(Centroid(x, w, self.id_counter))
            self.centroidList.sort(key = lambda c: c.mean)
            self.id_counter += 1
        self.n += 1

    def quantile(self, x):
        if len(self.centroidList) < 3:
            return 0.0
        total_weight = sum([centroid.count for centroid in self.centroidList])
        q = x * total_weight
        m = len(self.centroidList)
        cumulated_weight = 0
        for nr in range(m):
            current_weight = self.centroidList[nr].count
            if cumulated_weight + current_weight > q:
                if nr == 0:
                    delta = self.centroidList[nr + 1].mean - self.centroidList[nr].mean
                elif nr == m - 1:
                    delta = self.centroidList[nr].mean - self.centroidList[nr - 1].mean
                else:
                    delta = (self.centroidList[nr + 1].mean -  self.centroidList[nr - 1].mean) / 2
                return self.centroidList[nr].mean + ((q - cumulated_weight) / (current_weight) - 0.5) * delta
            cumulated_weight += current_weight
        return self.centroidList[nr].mean

    def _closest_centroids(self, x):
        S = []
        z = None
        for centroid in self.centroidList:
            d = centroid.distance(x)
            if z == None:
                z = d
                S.append(centroid)
            elif z == d:
                S.append(centroid)
            elif z > d:
                S = [centroid]
                z = d
            elif x > centroid.mean:
                break
        T = []
        for centroid in S:
            q = self._centroid_quantile(centroid)
            if centroid.count + 1 <= 4 * self.n * self.delta * q * (1 - q):
                T.append(centroid)
        return T

    def _centroid_quantile(self, c):
        q = 0
        for centroid in self.centroidList:
            if centroid.equals(c):
                q += c.count / 2
                break
            else:
                q += centroid.count
        return q / sum([centroid.count for centroid in self.centroidList])

    def __len__(self):
        return len(self.centroidList)

    def __repr__(self):
        return '[ %s ]' % ', '.join([str(c) for c in self.centroidList])

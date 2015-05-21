import sys
import unittest

from sure import expect

sys.path.append('../')
from lib.modules import models


class TestModels(unittest.TestCase):

    def setUp(self):
        self.name = 'cpu'
        self.timestamp = 1234
        self.value = 66.6
        self.ttl = 60
        self.tst = models.TimeSeriesTuple(self.name, self.timestamp, self.value)
        self.tsv = models.TSV(self.tst)
        self.csv = models.CSV(self.tst)
        self.redis_lastval = models.RedisLastValue(self.tst, self.ttl)
        self.redis_timestamped = models.RedisTimestamped(self.tst, self.ttl)

    def test_TimeSeriesTuple(self):
        exp_str = 'TimeSeriesTuple: name=%s timestamp=%d value=%f' % (self.name, self.timestamp, float(self.value))
        str(self.tst).should.be.equal(exp_str)
        self.tst.transform().should.be.equal(self.tst)

    def test_TSV(self):
        exp_str = '%s\t%d\t%f' % (self.name, self.timestamp, float(self.value))
        str(self.tsv).should.be.equal(exp_str)
        self.tsv.transform().should.be.equal(self.tst)

    def test_CSV(self):
        exp_str = '%s,%d,%f' % (self.name, self.timestamp, float(self.value))
        str(self.csv).should.be.equal(exp_str)
        self.tsv.transform().should.be.equal(self.tst)

    def test_RedisLastValue(self):
        key = self.redis_lastval.build_name(self.tst)
        key.should.be.equal(self.name)
        exp_str = '%s,%d,%f with TTL: %d' % (key, self.timestamp, self.value, self.ttl)
        str(self.redis_lastval).should.be.equal(exp_str)

    def test_RedisTimestamped(self):
        key = self.redis_timestamped.build_name(self.tst)
        key.should.be.equal('%s:%s' % (self.name, self.timestamp))
        exp_str = '%s,%d,%f with TTL: %d' % (key, self.timestamp, self.value, self.ttl)
        str(self.redis_timestamped).should.be.equal(exp_str)

import sys
import unittest

sys.path.append("../")

from mock import Mock
from sure import expect

from fixtures.config import CONFIG
from lib.modules import models
from lib.modules import sink


class TestInterface(object):

    class TestSinkInterface(unittest.TestCase):

        def setUp(self):
            self.my_sink = sink.Sink()

        def tearDown(self):
            self.my_sink = None

        def test_sink_properties(self):
            self.my_sink.should.have.property('connect')
            self.my_sink.should.have.property('write')
            self.my_sink.should.have.property('close')

        def test_sink_write(self):
            self.my_sink.connect.when.called_with().should.throw(
                NotImplementedError)


class TestRedisSinkInterface(TestInterface.TestSinkInterface):

    def stub_setex(self, metric_name, ttl, metric_value):
        self.redis_pipeline_list.append((metric_name, ttl, metric_value))

    def stub_execute(self):
        self.redis_pipeline_list = []

    def setUp(self):
        sink.redis = Mock()
        self.configuration = CONFIG["SINK"]["RedisSink"]
        self.my_sink = sink.RedisSink(self.configuration)
        self.redis_pipeline_list = []

    def test_sink_write(self):
        self.my_sink.redis_pipeline.setex = self.stub_setex
        self.my_sink.redis_pipeline.execute = self.stub_execute
        data_tuple = models.TimeSeriesTuple('service', 60, 1.0)
        for nr_elements_to_insert in [9, 11]:
            redis_value = [models.RedisLastValue(data_tuple, 60)] * nr_elements_to_insert
            self.my_sink.write(redis_value)
            expected_pipeline = [('service', 60, 1.0)] * (nr_elements_to_insert % self.configuration["pipeline_size"])
            expect(self.redis_pipeline_list).to.equal(expected_pipeline)
            self.redis_pipeline_list = []

    def test_sink_properties(self):
        super(TestRedisSinkInterface, self).test_sink_properties()
        self.my_sink.should.have.property('host')
        self.my_sink.should.have.property('port')
        self.my_sink.should.have.property('pipeline_size')
        self.my_sink.should.have.property('connection')


class TestRedisClusterSinkInterface(TestRedisSinkInterface):

    def setUp(self):
        sink.rediscluster = Mock()
        self.configuration = CONFIG["SINK"]["RedisClusterSink"]
        self.my_sink = sink.RedisClusterSink(self.configuration)
        self.redis_pipeline_list = []

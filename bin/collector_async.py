import re
import sys
from functools import partial

from twitter.common import app, log
sys.path.append("../")

from lib.modules import spout, config, bijection, models, event_emitter_2
from lib.modules import sink


app.add_option("--incoming_connector", default="CarbonAsyncTcpSpout",
               help="Select the incoming metric connection interface")
app.add_option("--metricstore_connector", default="AerospikeSink",
               help="Select the metricstore connection interface")
app.add_option("--host", default="127.0.0.1", help="Connection Host")
app.add_option("--port", default=2003, help="Connection Port", type="int")
app.add_option("--config", help="Collector Config")

EE = event_emitter_2.EventEmitter2()
ROUTER_CONFIG = None
WHITELIST = None
BLACKLIST = None


def setup(options):
    print "in setup"
    ROUTER_CONFIG = config.load(options.config)
    WHITELIST = ROUTER_CONFIG['ROUTER']['whitelist']
    BLACKLIST = [re.compile(x) for x in ROUTER_CONFIG['ROUTER']['blacklist']]
    for pattern, mapping in WHITELIST.iteritems():
        for model, default in mapping.iteritems():
            handler = partial(getattr(models, model), defaults=default)
            EE.add_listener(pattern, handler, count=-1)

def reject(metric):
    for pattern in BLACKLIST:
        if re.match(pattern, metric.name):
            continue
    yield metric


def stream(writer, metric):
    for m in EE.emit(metric.name, {"datapoint": metric}):
        writer.write(m)


def main(args, options):
    setup(options)

    try:
        pass
        #redis_writer = getattr(sink, options.metricstore_connector)({"host": "127.0.0.1", "port": 6379})# {"path": "/opt/anna-molly/test_file.tsv"}})
    except AttributeError:
        log.error("Could not find metricstore connector interface %s" %(options.metricstore_connector))

    try:
        aerospike_writer = getattr(sink, options.metricstore_connector)({ "hosts": [ ( '10.0.100.117', 3000 ), ( '10.0.100.118', 3000 ) ], "policies": { "timeout": 1000 } })
        aerospike_writer.connect()

    except AttributeError:
        log.error("Could not find metricstore connector interface %s" %(options.metricstore_connector))

    try:
        listener_config = {'spout':{'carbon':{'host': "0.0.0.0", 'port': 2003}}}
        listener = getattr(spout, options.incoming_connector)(listener_config, partial(stream, aerospike_writer))
        listener.connect()
    except AttributeError:
        log.error("Could not find connector interface %s" %(options.incoming_connector))

app.main()

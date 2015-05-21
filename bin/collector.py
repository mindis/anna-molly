import re
import sys
from functools import partial

from twitter.common import app, log
sys.path.append("../")

from lib.modules import spout, config, models, event_emitter_2
from lib.modules import sink


app.add_option("--incoming_connector", default="CarbonAsyncTcpSpout",
               help="Select the incoming metric connection interface")
app.add_option("--metricstore_connector", default="RedisSink",
               help="Select the metricstore connection interface")
app.add_option("--host", default="127.0.0.1", help="Connection Host")
app.add_option("--port", default=2003, help="Connection Port", type="int")
app.add_option("--config", help="Collector Config")

# Globals
EE = event_emitter_2.EventEmitter2()
ROUTER_CONFIG = None
WHITELIST = None
BLACKLIST = None


def setup(options):
    # TODO Load config generally
    ROUTER_CONFIG = config.load(options.config)
    WHITELIST = ROUTER_CONFIG['router']['whitelist']
    log.debug("Whitelist: %s" % (WHITELIST))
    BLACKLIST = [re.compile(x) for x in ROUTER_CONFIG['router']['blacklist']]
    log.debug("Blacklist: %s" % (BLACKLIST))
    for pattern, mappings in WHITELIST.iteritems():
        for _models in mappings:
            for model, default in _models.iteritems():
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
        log.debug("Trying to connect to %s writer @\n%s" % (
            options.metricstore_connector, {"host": "127.0.0.1", "port": 6379}))
        writer = getattr(sink, options.metricstore_connector)(
            {"host": "127.0.0.1", "port": 6379})
    except AttributeError:
        log.error("Could not find metricstore connector interface %s" %
                  (options.metricstore_connector))
    try:
        listener_config = {
            'spout': {'carbon': {'host': "0.0.0.0", 'port': 2014}}}
        listener = getattr(spout, options.incoming_connector)(
            listener_config, partial(stream, writer))
        listener.connect()
    except AttributeError:
        log.error("Could not find connector interface %s" %
                  (options.incoming_connector))

app.main()

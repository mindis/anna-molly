import re
import sys
from functools import partial

from twitter.common import app, log

sys.path.append("../")

from lib.modules import spout
from lib.modules import sink
from lib.modules import config
from lib.modules import bijection
from lib.modules import models



app.add_option("--incoming_connector", default="CarbonTcpSpout",
               help="Select the incoming metric connection interface")
app.add_option("--metricstore_connector", default="FileSink",
               help="Select the metricstore connection interface")
app.add_option("--host", default="127.0.0.1", help="Connection Host")
app.add_option("--port", default=2003, help="Connection Port", type="int")
app.add_option("--config", default="config/collector.json", help="Collector Config")


def find(f, seq):
    for i in seq:
        if f(i):
            return i


def match_builder(whitelist):
    # Okay, this is brainfuck
    return {re.compile(key): [partial(getattr(models, k), **v) for k, v in value.iteritems()] for key, value in whitelist.iteritems()}


def reject(blacklist, metrics):
    for metric in metrics:
        for pattern in blacklist:
            if re.match(pattern, metric.name):
                continue
        yield metric


def router(builder, metrics):
    for metric in metrics:
        # This is overkill. Since we assume that there will be only one match
        match_models = find(lambda x: x.match(metric.name), builder.keys())
        for m in builder[match_models]:
            yield bijection.inject(metric, m)


def main(args, options):
    ROUTER_CONFIG = config.load(options.config)
    builder = match_builder(ROUTER_CONFIG['whitelist'])
    blacklist = [re.compile(x) for x in ROUTER_CONFIG['blacklist']]
    try:
        listener_config = {'spout':{'carbon':{'host': "127.0.0.1", 'port': 2004, 'model': "pickle"}}}
        listener = getattr(spout, options.incoming_connector)(listener_config)
    except AttributeError:
        log.error("Could not find connector interface %s" %(options.incoming_connector))

    try:
        writer = getattr(sink, options.metricstore_connector)({"file": {"path": "/opt/anna-molly/test_file.tsv"}})
    except AttributeError:
        log.error("Could not find metricstore connector interface %s" %(options.metricstore_connector))

    metrics = listener.stream()
    metrics = reject(blacklist, metrics)
    metrics = router(builder, metrics)
    
    writer.write(metrics)
    writer.close()

app.main()

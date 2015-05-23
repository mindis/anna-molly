CONFIG = {
    "SPOUT": {
        "CarbonTcpSpout": {
            "host": "127.0.0.1",
            "port": 2004,
            "model": "pickle"
        }
    },
    "SINK": {
        "RedisSink": {
            "host": "127.0.0.1",
            "port": 6379,
            "pipeline_size": 100
        }
    },
    "metric_store": {
        "host": "127.0.0.1",
        "port": 6379,
        "pipeline_size": 100
    },
    "sink": {
        "host": "127.0.0.1",
        "port": 6379,
        "prefix": "AnnaMolly"
    },
}

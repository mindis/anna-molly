* Goals
- Extensible framework to Collect, Analyse and React to Metrics.
- Providing interfaces to:
    - Collect data in Real-Time/Batch Mode.
    - Run Anomaly-Detection algorithms independently and trigger events.
    - Event handlers that react to events.

* Default interfaces implement will be:
    - DataSource: Carbon(Real-time)/Graphite(Batch)
    - MetricStore: Redis
    - AlgorithmPlugin: OutlierDetectionPlugin, SeasonalAnalysisPlugin, CrossMappingAnalysisPlugin
    - DataSink: Graphite
X    - Event: SQS/SNS (Beyond Scope)
X    - EventHandlerPlugin:TerminateInstancePlugin (Beyond Scope)

* CollectorD will be a Daemon that recieves incoming metric data, transforms it into MetricStore data-model and writes to metricstore.

* Scheduler

* Utilities

*Milestones*


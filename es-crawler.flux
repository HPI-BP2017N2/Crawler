name: "crawler"

includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: false
      file: "crawler-conf.yaml"
      override: true

    - resource: false
      file: "access-data.yaml"
      override: true

    - resource: false
      file: "local-conf.yaml"
      override: true

    - resource: false
      file: "es-conf.yaml"
      override: true



spouts:
  - id: "spout"
    className: "com.digitalpebble.stormcrawler.elasticsearch.persistence.AggregationSpout"
    parallelism: 10

bolts:
  - id: "partitioner"
    className: "com.digitalpebble.stormcrawler.bolt.URLPartitionerBolt"
    parallelism: 1
  - id: "fetcher"
    className: "com.digitalpebble.stormcrawler.bolt.FetcherBolt"
    parallelism: 1
  - id: "sitemap"
    className: "com.digitalpebble.stormcrawler.bolt.SiteMapParserBolt"
    parallelism: 1
  - id: "parse"
    className: "com.digitalpebble.stormcrawler.bolt.JSoupParserBolt"
    parallelism: 1
  - id: "index"
    className: "de.hpi.bpStormcrawler.BPIndexerBolt"
    parallelism: 1
  - id: "status"
    className: "com.digitalpebble.stormcrawler.elasticsearch.persistence.StatusUpdaterBolt"
    parallelism: 1
  - id: "status_metrics"
    className: "com.digitalpebble.stormcrawler.elasticsearch.metrics.StatusMetricsBolt"
    parallelism: 1

  - id: "finishedDomain"
    className: "de.hpi.bpStormcrawler.BPFinishedDomainBolt"
    parallelism: 1

  - id: "HTMLstore"
    className: "de.hpi.bpStormcrawler.BPRabbitMQBolt"
    constructorArgs:
        # exchangeName
          - "crawler"
        #routingName
          - "crawledPages"
    parallelism: 1

  - id: "finishedShopStore"
    className: "de.hpi.bpStormcrawler.BPRabbitMQBolt"
    constructorArgs:
        # exchangeName
          - "crawler"
        #routingName
          - "doneMessagesToMatch"
    parallelism: 1



streams:
  - from: "spout"
    to: "partitioner"
    grouping:
      type: SHUFFLE

  - from: "spout"
    to: "status_metrics"
    grouping:
      type: SHUFFLE

  - from: "spout"
    to: "finishedDomain"
    grouping:
        type: SHUFFLE

  - from: "partitioner"
    to: "fetcher"
    grouping:
      type: FIELDS
      args: ["url"]

  - from: "fetcher"
    to: "sitemap"
    grouping:
      type: LOCAL_OR_SHUFFLE

  - from: "sitemap"
    to: "parse"
    grouping:
      type: LOCAL_OR_SHUFFLE

  - from: "parse"
    to: "index"
    grouping:
      type: LOCAL_OR_SHUFFLE

  - from: "fetcher"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"

  - from: "sitemap"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"

  - from: "parse"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"

  - from: "index"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"

  - from: "index"
    to: "HTMLstore"
    grouping:
        type: LOCAL_OR_SHUFFLE
        streamId: "storage"

  - from: "finishedDomain"
    to: "finishedShopStore"
    grouping:
        type: LOCAL_OR_SHUFFLE
        streamId: "finishedDomainNotification"

# Logstash Indexer Helm Chart

This chart deploys a Logstash instance that consumes events from Kafka, enriches them with data
fetched from a variety of sources, and indexes the resulting documents into either Solr or
Elasticsearch. Every part of the Logstash pipeline is rendered from `values.yaml`, so operators can
configure behaviour declaratively without touching raw pipeline syntax.

## Features

* Kafka input with SASL/SSL support and pluggable codec/options.
* Inline payload handling for events that already contain the full document.
* Hybrid enrichment via REST APIs, MongoDB lookups, or S3 object retrieval.
* Flexible field mapping that transforms payload fields to the exact schema expected by the target
  index.
* Selectable Solr (`solr_http`) or Elasticsearch output plugins with optional authentication.
* Rich Pod customisation, including extra containers, volumes, init containers, environment
  variables, and resource limits.

## Data enrichment modes

Configure the following sections under `pipeline` to control where Logstash should obtain the full
payload for each event:

* `pipeline.inline` – For events that already contain the full payload. The chart can optionally
  parse JSON strings, move nested payloads into the canonical payload field, and even duplicate root
  fields into the payload when only a subset of messages include inline data.
* `pipeline.dataSources.rest` – Issues HTTP requests to retrieve additional document fields. The
  request URL, headers, body, and response parsing (including JSON pointer extraction) are fully
  configurable from values. The chart wires the standard `http`, `json`, and `mutate` filters
  together so that REST enrichment remains declarative without any custom Ruby code.
* `pipeline.dataSources.mongo` – Uses the
  [`logstash-filter-mongodb`](https://github.com/logstash-plugins/logstash-filter-mongodb) plugin to
  load documents from MongoDB. Customize the query, projection, and result selection pointer.
* `pipeline.dataSources.s3` – Downloads objects from S3 using the AWS SDK. Supports optional
  role-assumption, JSON parsing, and pointer-based extraction.

Each data source supports `matchSourceTypes` (values read from `pipeline.sourceTypeField`) so that
individual events can declare how they should be enriched. Hybrid workflows are supported by
allowing events to fall back to inline payloads whenever they exist.

> **Note:** Fetching from MongoDB and Solr requires the corresponding Logstash plugins
> (`logstash-filter-mongodb` and `logstash-output-solr_http`). S3 enrichment depends on the
> `aws-sdk-s3` (and optionally `aws-sdk-sts`) Ruby gems. Install any required plugins by supplying
> `extraInitContainers` that run `logstash-plugin install ...` or by extending the Logstash image.

## Ruby helper scripts

Helper Ruby scripts live under `files/scripts` inside the chart and are mounted into the Logstash
pod at runtime. They encapsulate the inline payload handling, static field defaults, MongoDB result
merging, and S3 retrieval logic used by the `ruby` filters. Configuration still flows entirely from
`values.yaml` through `script_params`, keeping the scripts reusable while keeping event behavior
declarative.

## Field mappings

`pipeline.fieldMappings` describes how to transform payload fields into the final document that will
be sent to Solr or Elasticsearch. The simplest form is a map where keys are the source field names
from the payload and values are the destination field names:

```yaml
pipeline:
  fieldMappings:
    title: title_s
    body: body_txt
```

When more control is needed, assign an object with `target`, optional `sourcePath` (to read from a
nested payload path), and `keepSource` to retain the original field after mapping.

```yaml
pipeline:
  fieldMappings:
    summary:
      target: summary_t
      sourcePath: payload.metadata.summary
      keepSource: true
```

## Outputs

Select the target indexer via `output.type` (`elasticsearch` or `solr`). Each block exposes the most
commonly used settings, including credentials that are sourced from Kubernetes Secrets via helper
environment variables. Additional raw Logstash outputs can be appended by populating
`output.extraOutputs`.

## Installing the chart

Add the chart to your manifests (for example within a Git repository) and customise `values.yaml` to
match your Kafka, enrichment, and indexing requirements. Once ready, render or install the chart
with Helm:

```bash
helm install logstash-indexer ./charts/logstash-indexer -f my-values.yaml
```

Run `helm template` to inspect the generated Kubernetes manifests and Logstash pipeline before
applying them to a cluster.

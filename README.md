# Solr Index Analyzer Request Handler

## Overview
The Solr Index Analyzer is a custom request handler that inspects Lucene index structures for a Solr core and reports the bytes consumed by postings, DocValues, points, term vectors, stored fields, and kNN vectors per field and in aggregate. The handler executes the requested analyses against each segment, merges the segment-level findings, and returns an `analysis` block in the Solr response. Each entry includes field totals as well as per-file-extension breakdowns so you can see where index storage is used most heavily.

## Building the JAR
1. Ensure you have Java 17 or later available in your environment.
2. Run `./gradlew clean build` from the project root. The shaded JAR will be created under `build/libs/`.

## Installing in Solr
1. Copy the built JAR into a Solr classpath directory, such as `${SOLR_HOME}/lib` for a single core or `${SOLR_SERVER_HOME}/lib` for all cores.
2. Declare the library in `solrconfig.xml` if the directory is not already on Solr's classpath:
   ```xml
   <lib dir="${solr.install.dir:}/lib" regex="solr-index-analyzer-.*\.jar" />
   ```
   Adjust the `dir` and `regex` values to match where you copied the JAR.
3. Define a request handler entry in the same `solrconfig.xml` file:
   ```xml
   <requestHandler name="/index-analysis"
                   class="org.commrogue.IndexAnalyzerRequestHandler"
                   startup="lazy" />
   ```
4. Reload the core (or restart Solr) so the new handler is registered.

## Invoking the Handler
Send a request to the handler with the analyses you want to run. For example:
```bash
curl "http://localhost:8983/solr/your_core/index-analysis?analysis=all"
```
The response contains an `analysis` section with an `overall_total_size` entry and a per-field breakdown. Each field contains totals and the Lucene file extensions that contributed to that field's footprint.

## Request Parameters
| Parameter | Allowed values | Default | Description |
|-----------|----------------|---------|-------------|
| `analysis` | Comma-separated list of analysis identifiers: `invertedIndex`, `docValues`, `pointValues`, `termVectors`, `knnVectors`, `storedFields`, or the special value `all`. | (none) — if omitted, the handler returns an empty analysis block. | Chooses which index components to inspect. Combine multiple identifiers with commas. |
| `termAnalysisMode` | `blockSkipping`, `partialInstrumentation`, `fullInstrumentation` | `blockSkipping` | Controls inverted index inspection. `blockSkipping` uses BlockTree metadata when possible, `partialInstrumentation` mixes structural reads with limited I/O tracking, and `fullInstrumentation` forces exhaustive postings iteration. |
| `docValuesAnalysisMode` | `structural`, `structural_with_fallback`, `instrumented` | `structural_with_fallback` | Determines how DocValues sizes are derived. Structural modes parse codec metadata, optionally falling back to instrumentation when the codec is unsupported. `instrumented` always streams DocValues. |
| `pointValuesAnalysisMode` | `structural`, `structural_with_fallback`, `instrumented` | `structural_with_fallback` | Chooses the BKD tree sizing approach. Structural modes read Lucene 90 point metadata; instrumentation traverses point values when necessary. |
| `termVectorsAnalysisMode` | `structural`, `structural-with-fallback`, `instrumented` | `structural-with-fallback` | Selects the term vector estimator. Structural mode apportions reads by field, fallback tries structural parsing first, and `instrumented` fully tracks term vector I/O. |
| `vectorAnalysisMode` | `structural`, `structuralWithFallback`, `instrumented` | `structural` | Sets how kNN vector data is measured. Structural modes parse HNSW/flat/quantized metadata when available; instrumentation visits the vector values. |
| `storedFieldsAnalysisMode` | `structural`, `structural_with_fallback`, `instrumented` | `structural_with_fallback` | Controls stored field sizing. Structural mode parses Lucene 9 stored field metadata; instrumentation walks every document’s stored fields when required. |

## Understanding the Output
The `analysis` map begins with an `overall_total_size` entry that summarizes the combined size of all requested index components. Each field entry lists its total bytes and the contributions of each Lucene file extension (for example, `doc`, `dvd`, `kdd`, `fdt`) so you can identify the dominant index structures. Depending on the analysis type, additional metadata—such as HNSW hierarchy statistics for vector fields—is also included in the field section.

## Notes
- The handler requires Solr 9.9.0/Lucene 9.12.2 APIs and should be deployed alongside compatible cores.
- Running full instrumentation modes can be expensive on large indexes. Start with the structural defaults and escalate only when you need deeper detail.

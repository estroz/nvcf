# Demo monitoring

The monitoring release collects Prometheus metrics from the gateway, request router, Pylon pods, Pylon Operator and GLM backend. OpenTelemetry Collector sends the samples to local VictoriaMetrics. Grafana loads the data source and LLM routing dashboard from ConfigMaps.

```text
Component /metrics endpoints
  Collector (pod discovery, one replica)
    VictoriaMetrics (three-day retention)
      Grafana (provisioned dashboard)
```

## Install

The example configuration and `spark.py init` enable monitoring. `spark.py stack` installs it after the routing services. Older configuration files that omit `monitoring` keep it disabled.

The three monitoring Deployments run on `nodes.control`. Default resource requests total 300 millicores and 512 MiB of memory. Memory limits total 1792 MiB. VictoriaMetrics uses a separate 5 GiB PVC. Grafana stores its local database on an ephemeral volume and reloads the dashboard and data source after restart. Save dashboard changes in the repository.

Monitoring uses namespace-scoped read-only pod discovery. It does not install cluster-wide operators or change the model release. Services are ClusterIP, and the dashboard command forwards Grafana to loopback. Grafana requires authentication. Credentials are generated once, stored in a Helm-managed Secret and saved locally with mode 0600. Treat the generated Helm values and work directory as secret material.

To add monitoring to an existing installation, attach using the matching recipe, edit the saved configuration printed by `spark.py paths`, and add:

```json
"monitoring": {"enabled": true}
```

Preload the monitoring images if the saved pull policy is `Never`, then run:

```bash
python3 spark.py monitoring
python3 spark.py verify-monitoring
python3 spark.py dashboard --port 13000
```

Wait for two 15-second scrapes before verification. The monitoring command changes only its own release. Existing gateway pods are scraped directly on port 9464, including when the metrics Service port is disabled. Fresh stack installations also enable gateway and router metrics Service ports.

Grafana opens at `http://127.0.0.1:13000/d/llm-demo`. Sign in as `admin` with the password in the work directory's `grafana-admin-password` file. Keep the dashboard command running while browsing. Press Ctrl-C to close its tunnel.

## Configuration

The recipe accepts these optional `monitoring` settings:

| Setting | Default | Purpose |
| --- | --- | --- |
| `enabled` | false when omitted | Install monitoring during `stack` |
| `imagePullPolicy` | Application pull policy | `Never`, `IfNotPresent` or `Always` |
| `images` | Chart version pins | Overrides for `collector`, `victoriaMetrics` and `grafana` |
| `retentionPeriod` | `3d` | VictoriaMetrics retention duration |
| `storageSize` | `5Gi` | Initial metrics PVC size |
| `namespaces` | Installation namespace | Existing namespaces to discover and grant pod-read access in |
| `extraTargets` | `[]` | Additional runtime or relay metric targets |

Image pins and default resources are defined in `charts/monitoring/values.yaml`, a JSON-formatted YAML file shared with the Python runner. Image overrides must use explicit version tags or digests. The Docker archive helpers require version tags. The runner reuses the installation's storage class and control node placement.

Each extra target requires a unique `name`, Kubernetes label `selector` and named container port `portName`. Optional `port` overrides the scrape port, and `path` defaults to `/metrics`. The named port selects one discovery target per pod. Add the recipe namespace to `namespaces` when it differs from the gateway namespace. For example:

```json
{
  "name": "second-backend",
  "selector": "app.kubernetes.io/name=second-model",
  "portName": "http"
}
```

The installed Pylon Operator must already watch any additional recipe namespace. Monitoring does not change its watch configuration. If the topology includes a separate Stargate Kubernetes relay, add its metrics target too.

Setting `enabled=false` stops future installation through the recipe. It does not uninstall an existing monitoring release. Uninstall and PVC cleanup are separate operator actions. The metrics PVC is retained by Helm, and model volumes belong to their own release.

## Offline images

While online, export the pinned ARM64 images:

```bash
python3 spark.py monitoring-images
python3 spark.py export-monitoring-images --archive /path/to/monitoring-arm64-images.tar
```

Transfer the archive to the installation workstation and import it into the control node using the existing recipe importer:

```bash
python3 spark.py import-monitoring-images --archive /path/to/monitoring-arm64-images.tar --allow-containerd-import
python3 spark.py monitoring
```

The importer checks the configured image tags and archive checksum before importing. Monitoring imports allow archives below 2 GiB and reserve a 2 GiB upload volume plus a 2 GiB download volume. Application-only imports retain their 1 GiB limit. It uses the image-loader's Python and K3s helper images, which must already be available for an offline import. Alternatively preload images through the cluster's normal runtime tooling. Set `monitoring.imagePullPolicy` to `Never` for an offline startup test. The monitoring chart is local and has no downloadable Helm dependencies. Grafana update checks and automatic plugin preinstallation are disabled.

Review the external artifact terms in [NOTICE](NOTICE), including Grafana OSS's AGPL-3.0 license, before distributing image bundles.

## Verification

`verify-monitoring` checks fresh successful scrapes for every configured component and matches all running selected pods to their collected series. It also checks the provisioned Grafana dashboard. Results go to `evidence/monitoring.json`. It fails on missing components, stale samples or any failed target.

```bash
python3 spark.py verify-monitoring --verify-traffic
```

The traffic option additionally runs the existing gateway acceptance client. It waits up to 75 seconds for the GLM request count, first-token histogram count and both streaming/nonstreaming completion-token counters to increase. Metric collection checks alone do not prove those request metrics work.

For an authorized failure rehearsal, keep Grafana open while running the recipe's recovery workflow. Observe registration, tunnels, router availability and request errors during interruption and recovery. A successful monitoring render or CPU fixture does not establish a real GLM recovery or offline Spark startup.

## Dashboard semantics

- Gateway counters provide user-visible request and token totals. Router and backend panels show their own work and are not added to gateway totals.
- Token rates reflect usage reported by completed requests. They are aggregate throughput, not an individual request's decoding speed.
- Gateway request latency includes the response duration. Router proxy latency measures upstream first byte.
- Registration, reverse tunnel connectivity and routability are separate signals. Operator condition panels show aggregate counts, while registration also has a namespace/name endpoint label.
- Health panels hide samples older than 45 seconds. Missing data stays unknown. The scrape-age panel shows stopped collection separately from a failed scrape.
- The model filter applies to model-labelled gateway/router metrics. Operator and Pylon panels retain endpoint/pod labels. Rejected requests can have no model label and remain visible in the errors panel.
- Backend queue and generation panels use the pinned llama.cpp schema. Other engines need queries for their own exported names. Their raw metrics remain available in Grafana Explore.

Logs and Kubernetes Events remain available through kubectl with an explicit context. This metrics release does not provision a log or trace store.

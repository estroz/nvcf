# Demo monitoring

The monitoring release collects Prometheus metrics from the gateway, request router, Pylon pods, Pylon Operator and GLM backend. OpenTelemetry Collector sends the samples to local VictoriaMetrics. Grafana loads the data source and LLM routing dashboard from ConfigMaps.

```text
Component /metrics endpoints
  Collector (pod discovery, one replica)
    VictoriaMetrics (three-day retention)
      Grafana (provisioned dashboard)
```

## Install

Run from `deploy/helm/llm-routing/spark` with the same context/configuration selection used for installation. New configurations enable monitoring during `stack`. For an existing deployment, attach if this workstation has no saved configuration, then locate it:

```bash
python3 spark.py attach-existing
python3 spark.py paths
```

Add this setting to the saved configuration:

```json
"monitoring": {"enabled": true}
```

[Preload the monitoring images](#offline-images) if the pull policy is `Never`, then install monitoring without changing the serving workloads:

```bash
python3 spark.py monitoring
```

## Verification

After GLM is registered, run:

```bash
python3 spark.py verify-monitoring --verify-traffic
```

The command waits up to 75 seconds for fresh scrapes, then checks the dashboard and metric increases from real GLM requests. Results are saved in `evidence/monitoring.json`. Omit `--verify-traffic` to check collection without sending inference requests.

## Dashboard

```bash
python3 spark.py dashboard --port 13000
```

Open `http://127.0.0.1:13000/d/llm-demo`. Sign in as `admin` using the work directory's `grafana-admin-password` file. Keep the command running. Ctrl-C closes the tunnel.

## Uninstall

Run the whole block from the recipe directory in the same configured terminal used for installation. If you used `--context`, `--config` or `--work-dir`, pass the same options before `context` in the lookup. Replace the default namespace and release name if customized. The monitoring release name is `releasePrefix` plus `-monitoring`.

```bash
(
  set -eu
  context="$(python3 spark.py context)"
  : "${context:?Context lookup returned an empty value}"
  namespace=llm-spark-poc
  : "${namespace:?Set the namespace from your saved configuration}"

  helm --kube-context "$context" -n "$namespace" uninstall llm-poc-monitoring --ignore-not-found --wait --timeout 3m
)
```

This removes monitoring only, skips an absent release and stops on other failures. The metrics PVC and local work files remain. Reinstalling generates a new Grafana password.

For a full demo teardown, remove monitoring above, then follow the [routing uninstall and reinstall instructions](../README.md#uninstall). Model volumes and downloaded files remain. To restore monitoring alone, follow [Install](#install).

## Configuration

The three monitoring Deployments run on `nodes.control`. Default requests total 300 millicores and 896 MiB of memory, with 2304 MiB of memory limits. Grafana requests 512 MiB and allows 1 GiB. VictoriaMetrics has a separate 5 GiB PVC. Grafana reloads its provisioned dashboard and data source after restart. Save dashboard changes in the repository.

Pod discovery uses namespace-scoped read-only permissions. Services are ClusterIP. Grafana credentials are generated once, stored in a Helm-managed Secret and saved locally with mode 0600. Keep generated values and the work directory private.

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
| `networkPolicy` | Disabled | Restrict monitoring pod egress to cluster pods and explicit API server hosts |

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

Setting `enabled=false` stops future installation through the recipe. Use [Uninstall](#uninstall) to remove the existing release while keeping its metrics PVC.

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

For an isolated offline check, set `monitoring.networkPolicy.enabled=true` and supply `monitoring.networkPolicy.apiServerCIDRs` with the Kubernetes API Service and endpoint IPs as `/32` or `/128` host CIDRs. The policy selects only this monitoring release and permits cluster pod traffic, including DNS, plus API access on TCP 443/6443. It requires a network plugin that enforces egress policies. Confirm a previously reachable external endpoint becomes unreachable, then restart monitoring with pull policy `Never` and repeat verification. Other policies can add allowed egress, so inspect them too. This verifies monitoring pod startup with external egress blocked, not a disconnected-node boot or model download.

## Verification details

Verification matches every running selected pod to fresh collected series and fails on missing components, stale samples or failed targets. Existing gateway pods are scraped directly on port 9464 even when the metrics Service port is disabled.

Traffic verification runs the gateway acceptance client and waits up to 75 seconds for per-model request counters, latency and first-token histogram counts/sums, and streaming/nonstreaming prompt/completion token counters to increase.

For an authorized failure rehearsal, keep Grafana open during the recipe's recovery workflow. Observe registration, tunnels, router availability and request errors during interruption and recovery. A successful render or CPU fixture does not establish real GLM recovery or offline Spark startup.

## Dashboard semantics

- Gateway counters provide user-visible request and token totals. Router and backend panels show their own work and are not added to gateway totals.
- Token rates reflect usage reported by completed requests. They are aggregate throughput, not an individual request's decoding speed.
- Gateway request latency includes the response duration. Router proxy latency measures upstream first byte.
- Registration, reverse tunnel connectivity and routability are separate signals. Operator condition panels show aggregate counts, while registration also has a namespace/name endpoint label.
- Health panels hide samples older than 45 seconds. Missing data stays unknown. The scrape-age panel shows stopped collection separately from a failed scrape.
- The model filter applies to model-labelled gateway/router metrics. Operator and Pylon panels retain endpoint/pod labels. Rejected requests can have no model label and remain visible in the errors panel.
- Backend queue and generation panels use the pinned llama.cpp schema. Other engines need queries for their own exported names. Their raw metrics remain available in Grafana Explore.

Logs and Kubernetes Events remain available through kubectl with an explicit context. This metrics release does not provision a log or trace store.

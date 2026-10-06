# One Kubernetes cluster across three DGX Sparks

This recipe installs native k3s on three GB10 Sparks. Initialize the first server once, then join the other two to it. Each node runs the control plane, embedded etcd, and GPU workloads. Three etcd members tolerate one member being unavailable. Two members do not provide that fault tolerance.

The [setup helper](../../../tools/scripts/spark-k3s) previews installation by default. `--apply` installs on the local machine. Its `inspect` and `verify` commands are read-only. Existing k3s state is refused. It does not update firmware, change network addresses, create user accounts, or install an inference stack.

## Before starting

Prepare three Sparks with working ARM64 Linux, GB10 drivers, NVIDIA Container Toolkit, systemd, Bash, `curl`, `jq`, and `iproute2`. Obtain administrator SSH access to each. Use the [firmware and OS checklist](firmware.md) before scheduling installation on new hardware.

Assign a unique hostname and a stable management IPv4 address to each machine through your network administrator, DHCP reservations, or your OS network settings. Configure addresses from a console if changing the interface carrying SSH. Keep the management network separate from the QSFP/RoCE fabric used for distributed GPU traffic.

Replace this documentation-only example with your own addresses and interfaces:

| Machine | Node name | Management IPv4 | Management interface |
| --- | --- | --- | --- |
| First server | `spark-a` | `192.0.2.11` | `eth0` |
| Second server | `spark-b` | `192.0.2.12` | `eth0` |
| Third server | `spark-c` | `192.0.2.13` | `eth0` |

All nodes must reach each other on TCP 6443, 2379-2380 and 10250, and UDP 8472. Restrict these to the cluster network. The k3s defaults use pod CIDR `10.42.0.0/16` and service CIDR `10.43.0.0/16`. These must not overlap your LAN, VPN, or fabric. This helper uses those defaults. See [k3s requirements](https://docs.k3s.io/installation/requirements) for network and host requirements.

Outbound HTTPS is required for the public k3s release and container images. This is an online recipe. Before using machines already hosting applications, inventory their workloads and volumes. Use their maintenance process instead of this fresh-install helper.

## 1. Inspect each Spark

Use the same checkout/revision on all three machines. From its repository root:

```bash
./tools/scripts/spark-k3s inspect
ip -br -4 address
ip route
timedatectl status
```

Check that the GPU is GB10, the management address is on the intended interface, clocks are synchronized, and `nvidia-container-runtime` is available. Resolve missing drivers or runtime packages using the [NVIDIA Container Toolkit guide](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html). Do not install a generic server driver over the vendor's Spark OS stack.

The helper uses k3s `v1.36.4+k3s1`. Its installer is pinned to upstream commit `4dedb15be78017a8ddd5b9e81acd44f3481078ed` and checked against a recorded SHA-256 before execution. The upstream installer also checks the downloaded k3s binary against the release checksum. Use the same helper revision on all servers.

## 2. Initialize the first server

On `spark-a`, preview the command with your real values:

```bash
./tools/scripts/spark-k3s init \
  --node-name spark-a --node-ip 192.0.2.11 --interface eth0
```

Review the plan, then run it on that machine:

```bash
sudo ./tools/scripts/spark-k3s init \
  --node-name spark-a --node-ip 192.0.2.11 --interface eth0 --apply
sudo k3s kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml \
  --context default get nodes -o wide
```

Wait for `spark-a` to be `Ready` with `control-plane,etcd` roles. Only this initialization uses `--cluster-init`. The administrative kubeconfig is root-readable (`600`).

Retrieve the secure server join token from the first server in a private terminal:

```bash
sudo cat /var/lib/rancher/k3s/server/node-token
```

Transfer it through your approved secret-sharing method. Keep the token out of command-line arguments, Git, and shared logs.

## 3. Join the second and third servers

On `spark-b`, save the token in a private file through a Bash prompt:

```bash
umask 077
read -rsp 'First-server token: ' SPARK_JOIN_TOKEN
printf '\n'
printf '%s\n' "$SPARK_JOIN_TOKEN" > "$HOME/spark-join-token"
unset SPARK_JOIN_TOKEN
```

Preview, then apply:

```bash
./tools/scripts/spark-k3s join \
  --node-name spark-b --node-ip 192.0.2.12 --interface eth0 \
  --server-ip 192.0.2.11 --token-file "$HOME/spark-join-token"
sudo ./tools/scripts/spark-k3s join \
  --node-name spark-b --node-ip 192.0.2.12 --interface eth0 \
  --server-ip 192.0.2.11 --token-file "$HOME/spark-join-token" --apply
```

Repeat on `spark-c` using node name `spark-c` and its management IP. Both nodes join the same first server. The helper retains a root-only token copy at `/etc/rancher/k3s/join-token` for the k3s service. After success, remove the temporary `$HOME/spark-join-token` file.

From the first server:

```bash
sudo k3s kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml \
  --context default get nodes -o wide
```

Expect exactly the three intended nodes, all Ready, on the same k3s version, with `control-plane,etcd` roles. Stop if the membership differs. An independent initialization on every machine creates separate clusters.

## 4. Enable GPU scheduling once per cluster

Run this step on the first server. Install Helm 3 or 4 using the [Helm installation guide](https://helm.sh/docs/intro/install/) if it is not already available. The commands below use the root-readable server kubeconfig with sudo. An administrator workstation can instead use its own authorized kubeconfig and context, with the API endpoint set to a server management IP included in the certificate.

On the first server, set:

```bash
export SPARK_KUBECONFIG=/etc/rancher/k3s/k3s.yaml
export SPARK_CONTEXT=default
sudo k3s kubectl --kubeconfig "$SPARK_KUBECONFIG" --context "$SPARK_CONTEXT" get runtimeclass nvidia
sudo k3s kubectl --kubeconfig "$SPARK_KUBECONFIG" --context "$SPARK_CONTEXT" get daemonsets -A
sudo helm --kubeconfig "$SPARK_KUBECONFIG" --kube-context "$SPARK_CONTEXT" list -A
```

[k3s detects NVIDIA Container Runtime at startup](https://docs.k3s.io/advanced#nvidia-container-runtime). Resolve a missing `nvidia` RuntimeClass before installing the plugin. If a GPU Operator or device plugin already manages these nodes, inspect that installation instead of creating a second one.

For a fresh cluster, install the official [NVIDIA device plugin chart](https://github.com/NVIDIA/k8s-device-plugin/tree/v0.17.4) with the included [values](device-plugin-values.yaml):

```bash
sudo helm --kubeconfig "$SPARK_KUBECONFIG" --kube-context "$SPARK_CONTEXT" \
  install spark-device-plugin nvidia-device-plugin \
  --repo https://nvidia.github.io/k8s-device-plugin --version 0.17.4 \
  --namespace kube-system --values docs/dev/spark-cluster/device-plugin-values.yaml \
  --wait --timeout 5m
```

The values select ARM64 control-plane nodes, set RuntimeClass `nvidia`, and disable the chart's default discovery-label affinity. This assumes every node in this dedicated cluster is a Spark. Initialization errors fail visibly. See the [v0.17.4 release](https://github.com/NVIDIA/k8s-device-plugin/releases/tag/v0.17.4) for integrated-GPU support.

## 5. Verify membership and GPU resources

```bash
sudo ./tools/scripts/spark-k3s verify \
  --kubeconfig "$SPARK_KUBECONFIG" --context "$SPARK_CONTEXT" \
  --nodes spark-a,spark-b,spark-c
```

The check requires exact membership, Ready ARM64 control-plane/etcd nodes, the pinned k3s version, one GPU of capacity and allocatable resource per node, and RuntimeClass handler `nvidia`. Allocatable is the advertised resource total, not a count of currently idle GPUs.

A pass is infrastructure metadata evidence. It does not execute GPU work, test cross-node pod traffic, establish distributed model readiness, or measure NCCL. Before deploying a model, schedule your workload's GPU smoke test on each node with `runtimeClassName: nvidia` and `resources.limits.nvidia.com/gpu: 1`. Reserve GPUs first and verify successful completion and logs for every node.

## 6. Prepare the distributed GPU network

A single Kubernetes cluster can form over the management network. Distributed GPU workloads additionally need their inter-node transport configured.

Use NVIDIA's [three-Spark ring playbook](https://build.nvidia.com/spark/connect-three-sparks) for physical cabling, interface addressing, and fabric checks. Record which physical machine is Node 1, 2, and 3 before running its configuration scripts. Preserve the management default route and select the correct interface for Flannel. Do not copy addresses or interface names from another fleet.

Afterward, run the [NCCL playbook](https://build.nvidia.com/spark/nccl/overview). Validate the actual container image and Kubernetes device/network configuration used by your application. Host RDMA or NCCL success does not by itself verify NCCL inside Kubernetes pods. This recipe does not configure RDMA device exposure or install the LLM-routing stack.

## Troubleshooting and maintenance

| Symptom | Next check |
| --- | --- |
| Helper refuses an installation | Inspect existing `/etc/rancher/k3s`, `/var/lib/rancher/k3s`, services, and binary. Use maintenance/recovery procedures instead of deleting them. |
| Install fails or node stays NotReady | `sudo journalctl -u k3s --no-pager -n 100`, management routes/firewall, clock synchronization and registry connectivity. |
| Join fails | Use the first server's secure server token, check TCP 6443 and etcd ports, and confirm matching versions. |
| API loses availability with two servers | Finish a healthy three-server quorum. Avoid stopping a server while only two etcd members exist. |
| Device plugin Pending | Check the chart values, ARM64/control-plane labels, RuntimeClass and DaemonSet events. |
| No GPU capacity | Inspect plugin logs, runtime discovery and host `nvidia-smi`. Registration may take a short time after the plugin becomes Ready. |
| GPU pod Pending | Check GPU allocations, events, image architecture and registry access. Metadata readiness does not reserve a GPU for the test. |
| RoCE or NCCL failure | Follow the NVIDIA playbook's cabling, addressing, MTU and transport diagnostics. Keep firmware/OS changes in a maintenance window. |

k3s installs a node-local storage provisioner. Its volumes are not replicated across machines. Plan workload placement and data recovery accordingly. Keep backups and follow the upstream [etcd backup/restore](https://docs.k3s.io/datastore/backup-restore) and [upgrade](https://docs.k3s.io/upgrades) procedures for an existing cluster.

## Validation record

On October 5, 2026, read-only inspection of an existing three-Spark cluster found all nodes Ready on k3s `v1.36.4+k3s1`, Linux `6.17.0-1032-nvidia`, DGX OS `7.6.0`, driver `580.178.04` and NVIDIA Container Toolkit `1.20.1-1`. The firmware guide distinguishes this observation from vendor upgrade requirements.

The helper's preview, refusal paths, pinned installer checksum, verification failures, and Helm values are checked locally. The read-only `inspect` and `verify` commands are checked against the existing cluster. The new `init`/`join` installation path has not been executed on fresh hardware. No firmware upgrade, cluster change, or GPU workload test was performed for this contribution.

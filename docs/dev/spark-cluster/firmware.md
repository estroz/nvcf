# Spark OS and firmware checks

Complete this before installing Kubernetes on a new machine. Keep upgrades on an existing cluster in a planned maintenance window with workload and data recovery arrangements.

## Identify the hardware and installed software

From the repository root on each Spark:

```bash
./tools/scripts/spark-k3s inspect
```

This reads the OS/kernel, GPU driver, container toolkit, k3s version if present, DMI model/BIOS fields, NIC firmware exposed in sysfs, and RDMA link state. Missing firmware fields are not evidence that a device is current.

For firmware devices managed by fwupd, inspect locally with:

```bash
fwupdmgr get-devices
```

The output can include device identifiers. Keep the raw inventory local. `get-devices` does not establish whether newer firmware is available.

## Choose the vendor update path

For Founders Edition hardware, use the [NVIDIA OS and Component Update Guide](https://docs.nvidia.com/dgx/dgx-spark/os-and-component-update.html). NVIDIA recommends DGX Dashboard for OS, driver and firmware updates. The guide also provides a manual update sequence for administrators. That sequence upgrades software/firmware and reboots, so it is a separate operation from this recipe.

For a partner GB10 system, follow that manufacturer's model-specific procedure. The [Spark release notes](https://docs.nvidia.com/dgx/dgx-spark/release-notes.html) state that their component version table applies to Founders Edition and partner releases can differ. Do not apply a firmware image selected only by the name ConnectX-7 or by another machine's BIOS date.

Before upgrading, identify the exact device/model, review the applicable release notes, provide stable power, save work, and prepare recovery. After the required reboot, repeat the inventory and verify the host GPU and fabric before installing or resuming Kubernetes.

## Observed baseline, not minimum requirements

Read-only inspection on October 5, 2026 found the following on three existing GB10 Sparks:

| Component | Observed version |
| --- | --- |
| DGX OS package | `7.6.0` |
| Running kernel | `6.17.0-1032-nvidia` |
| GPU driver | `580.178.04` |
| Container Toolkit | `1.20.1-1` |
| k3s | `v1.36.4+k3s1` |
| DMI BIOS string | `5.36_0ACUM018` |
| ConnectX firmware from sysfs | `28.45.4028` |

The three servers were Ready and their RDMA links reported ACTIVE/LINK_UP. This does not establish performance, a tested firmware upgrade, or fresh-install qualification.

The public Founders Edition release notes reviewed that day list DGX OS `7.5.0`, driver `580.159.03` and UEFI `1.110.13`. The DMI BIOS string above is a different identifier from that UEFI component version. No direct equivalence or upgrade requirement was established. Use the exact device's update metadata and vendor guidance rather than ordering these strings as versions.

The setup helper pins k3s for reproducibility. It does not pin or downgrade the kernel, driver, or firmware to these observed values. Qualify a different vendor-supported OS baseline with host GPU checks, Kubernetes lifecycle tests and the application's distributed GPU test before adopting it across a cluster.

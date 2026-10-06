# Spark cluster documentation

Keep this guide public and specific to GB10 Sparks. Use official NVIDIA and k3s sources for hardware and installation requirements. Keep observed versions separate from required versions.

The install helper lives at `tools/scripts/spark-k3s`. Tests run with `bash tools/scripts/test/test-spark-k3s`. Run `markdownlint` on Markdown changes and `tools/ci/check-docs` for repository docs validation.

Never execute installation, Helm deployment, firmware updates or GPU tests during a documentation-only review. `inspect` and `verify` are read-only. Every Kubernetes command must specify kubeconfig and context.

# Kubernetes manifests

Kustomize manifests for one InputLayer engine per tenant:

- `base/`: StatefulSet (one replica), PVC, Service, ConfigMap
- `components/ingress/`: optional ingress-nginx Ingress
- `overlays/tenant-example/`: per-tenant overlay to copy
- `maintenance/`: offline volume access for backup and restore

Install steps, the single-writer guarantees and their limits, and the
backup/restore runbook are in the Kubernetes section of
`docs/content/docs/guides/deployment.mdx`.

Validate after editing: `make k8s-check` (needs kubectl and kubeconform).

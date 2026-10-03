#!/bin/bash
# Validate the Kubernetes manifests in deploy/kubernetes.
#
# Renders the base and the example tenant overlay with kustomize, validates
# them and the maintenance pod against the Kubernetes schemas (kubeconform
# -strict), and checks the invariants the docs promise: one replica, and the
# pod uid matching the uid the Dockerfile pins.
#
# Requires kubectl (for kustomize) and kubeconform on PATH.
#
# Usage:
#   ./scripts/check-k8s-manifests.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
K8S="$PROJECT_DIR/deploy/kubernetes"
KUBE_VERSION="${KUBE_VERSION:-1.30.0}"

for tool in kubectl kubeconform; do
    command -v "$tool" >/dev/null || { echo "ERROR: $tool not found on PATH" >&2; exit 2; }
done

fail() { echo "FAIL: $*" >&2; exit 1; }

validate() {
    kubeconform -strict -summary -kubernetes-version "$KUBE_VERSION" "$@"
}

for dir in base overlays/tenant-example; do
    echo "== $dir"
    rendered="$(kubectl kustomize "$K8S/$dir")"
    validate - <<<"$rendered"
    grep -qx '  replicas: 1' <<<"$rendered" || fail "$dir: StatefulSet must keep replicas: 1"
done
echo "== maintenance"
validate "$K8S/maintenance/maintenance-pod.yaml"

uid="$(sed -n 's/.*useradd -r -u \([0-9]*\) .*inputlayer$/\1/p' "$PROJECT_DIR/Dockerfile")"
[ -n "$uid" ] || fail "Dockerfile no longer pins the inputlayer uid"
for file in base/statefulset.yaml maintenance/maintenance-pod.yaml; do
    for key in runAsUser runAsGroup fsGroup; do
        grep -q "^ *$key: $uid\$" "$K8S/$file" || fail "$file: $key must be $uid (Dockerfile uid)"
    done
done

echo "OK: manifests valid, single replica, uid $uid consistent"

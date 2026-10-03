#!/usr/bin/env bash
# Install the pinned gitleaks release into target/tools/gitleaks after
# verifying its SHA-256. CI and `make install-gitleaks` use this, so local
# and CI secret scans run the same scanner version.
#
# Usage: scripts/install-gitleaks.sh
set -euo pipefail

VERSION=8.30.1
ROOT=$(git rev-parse --show-toplevel)
DEST=$ROOT/target/tools/gitleaks

case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) PLATFORM=linux_x64 SHA256=551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb ;;
    Linux-aarch64 | Linux-arm64) PLATFORM=linux_arm64 SHA256=e4a487ee7ccd7d3a7f7ec08657610aa3606637dab924210b3aee62570fb4b080 ;;
    Darwin-x86_64) PLATFORM=darwin_x64 SHA256=dfe101a4db2255fc85120ac7f3d25e4342c3c20cf749f2c20a18081af1952709 ;;
    Darwin-arm64) PLATFORM=darwin_arm64 SHA256=b40ab0ae55c505963e365f271a8d3846efbc170aa17f2607f13df610a9aeb6a5 ;;
    *) echo "install-gitleaks: no pinned release for $(uname -s)-$(uname -m); install gitleaks $VERSION yourself" >&2; exit 1 ;;
esac

if [ -x "$DEST" ] && [ "$("$DEST" version)" = "$VERSION" ]; then
    echo "gitleaks $VERSION already installed at $DEST"
    exit 0
fi

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
TARBALL=gitleaks_${VERSION}_${PLATFORM}.tar.gz
curl -fsSL -o "$TMP/$TARBALL" "https://github.com/gitleaks/gitleaks/releases/download/v$VERSION/$TARBALL"
if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL=$(sha256sum "$TMP/$TARBALL" | cut -d' ' -f1)
else
    ACTUAL=$(shasum -a 256 "$TMP/$TARBALL" | cut -d' ' -f1)
fi
if [ "$ACTUAL" != "$SHA256" ]; then
    echo "install-gitleaks: checksum mismatch for $TARBALL (expected $SHA256, got $ACTUAL)" >&2
    exit 1
fi
tar -xzf "$TMP/$TARBALL" -C "$TMP" gitleaks
mkdir -p "$(dirname "$DEST")"
mv "$TMP/gitleaks" "$DEST"
echo "Installed gitleaks $VERSION at $DEST"

//! Registry client: index fetch, version resolution, digest-verified download,
//! and the local ontology cache.
//!
//! The registry is Helm-shaped (see inputlayer/ontology-registry): `index.json`
//! maps ontology names to versions, each carrying a tarball URL and a sha256
//! digest. Nothing is unpacked or deployed unless the digest matches - a
//! mismatch is a hard refusal, because entries are rule packs the engine will
//! treat as ground truth.

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub const DEFAULT_INDEX_URL: &str =
    "https://raw.githubusercontent.com/inputlayer/ontology-registry/main/index.json";

#[derive(Debug, Deserialize)]
pub struct Index {
    #[serde(default)]
    pub entries: std::collections::BTreeMap<String, Vec<IndexEntry>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexEntry {
    pub version: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub engine: String,
    pub urls: Vec<String>,
    pub digest: String,
}

/// The subset of ontology.toml the CLI needs to deploy an entry.
#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub ontology: ManifestOntology,
}

#[derive(Debug, Deserialize)]
pub struct ManifestOntology {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub title: String,
    pub rules: Vec<String>,
}

pub struct Registry {
    index_url: String,
    token: Option<String>,
    client: reqwest::Client,
}

/// Registry names and versions end up in filesystem paths and IQL literals;
/// keep them to a boring charset so a hostile index cannot traverse or
/// inject. Rejects empty strings and leading dots.
pub fn validate_component(kind: &str, value: &str) -> Result<()> {
    let ok = !value.is_empty()
        && !value.starts_with('.')
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if ok {
        Ok(())
    } else {
        bail!("invalid {kind} {value:?}: allowed characters are [A-Za-z0-9._-]");
    }
}

fn host_of(url: &str) -> Option<String> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let mut host = &rest[..end];
    if let Some(at) = host.rfind('@') {
        host = &host[at + 1..];
    }
    let host = host.rsplit_once(':').map_or(host, |(h, _)| h);
    Some(host.to_ascii_lowercase())
}

impl Registry {
    pub fn new(index_url: String, token: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            index_url,
            token,
            client,
        }
    }

    /// The token is only ever sent to GitHub hosts or the index's own host -
    /// index entries can list arbitrary mirror URLs, and a bearer token must
    /// not follow them there.
    fn token_for(&self, url: &str) -> Option<&str> {
        let token = self.token.as_deref()?;
        let host = host_of(url)?;
        let github = matches!(
            host.as_str(),
            "github.com"
                | "api.github.com"
                | "raw.githubusercontent.com"
                | "objects.githubusercontent.com"
                | "release-assets.githubusercontent.com"
        );
        if github || Some(host) == host_of(&self.index_url) {
            Some(token)
        } else {
            None
        }
    }

    fn request(&self, url: &str) -> reqwest::RequestBuilder {
        let mut req = self.client.get(url).header("User-Agent", "il-cli");
        if let Some(token) = self.token_for(url) {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        req
    }

    pub async fn index(&self) -> Result<Index> {
        let resp = self
            .request(&self.index_url)
            .send()
            .await
            .with_context(|| format!("failed to fetch registry index: {}", self.index_url))?;
        if !resp.status().is_success() {
            bail!(
                "registry index fetch failed ({}): {} - private registries need IL_REGISTRY_TOKEN",
                resp.status(),
                self.index_url
            );
        }
        resp.json()
            .await
            .context("registry index is not valid JSON")
    }

    /// Resolve `name` or `name@version` against the index (latest = first listed).
    pub async fn resolve(&self, spec: &str) -> Result<(String, IndexEntry)> {
        let (name, version) = match spec.split_once('@') {
            Some((n, v)) => (n, Some(v)),
            None => (spec, None),
        };
        let index = self.index().await?;
        let versions = index
            .entries
            .get(name)
            .ok_or_else(|| anyhow!("ontology '{name}' not found in registry"))?;
        let entry = match version {
            Some(v) => versions
                .iter()
                .find(|e| e.version == v)
                .ok_or_else(|| anyhow!("ontology '{name}' has no version '{v}'"))?,
            None => versions
                .first()
                .ok_or_else(|| anyhow!("ontology '{name}' has no versions"))?,
        };
        validate_component("ontology name", name)?;
        validate_component("version", &entry.version)?;
        Ok((name.to_string(), entry.clone()))
    }

    /// Fetch into the cache (digest-verified) and return the unpacked entry
    /// dir. Cache writes are staged and renamed into place so an interrupted
    /// unpack never leaves a half-populated dir that later runs trust, and a
    /// cache hit is only honored when its recorded digest matches the index
    /// (a republished version is re-fetched, keeping pack_meta attribution
    /// honest).
    pub async fn fetch(&self, name: &str, entry: &IndexEntry) -> Result<PathBuf> {
        let version_dir = cache_dir()?.join(name).join(&entry.version);
        let entry_dir = version_dir.join(name);
        let digest_file = version_dir.join(".digest");
        if entry_dir.join("ontology.toml").is_file() {
            let recorded = std::fs::read_to_string(&digest_file).unwrap_or_default();
            if recorded.trim() == entry.digest {
                return Ok(entry_dir);
            }
            std::fs::remove_dir_all(&version_dir)
                .with_context(|| format!("cannot evict stale cache {}", version_dir.display()))?;
        }

        let url = entry
            .urls
            .first()
            .ok_or_else(|| anyhow!("index entry for {name}@{} has no urls", entry.version))?;
        let bytes = self.download(url).await?;

        let digest = format!("sha256:{:x}", Sha256::digest(&bytes));
        if digest != entry.digest {
            bail!(
                "digest mismatch for {name}@{}: index says {}, downloaded {}\nrefusing to install",
                entry.version,
                entry.digest,
                digest
            );
        }

        let parent = version_dir
            .parent()
            .ok_or_else(|| anyhow!("cache path has no parent"))?;
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create cache dir {}", parent.display()))?;
        let staging = parent.join(format!(".{}.tmp-{}", entry.version, std::process::id()));
        let _ = std::fs::remove_dir_all(&staging);
        std::fs::create_dir_all(&staging)?;
        let decoder = flate2::read::GzDecoder::new(std::io::Cursor::new(bytes));
        tar::Archive::new(decoder)
            .unpack(&staging)
            .context("failed to unpack ontology tarball")?;
        if !staging.join(name).join("ontology.toml").is_file() {
            let _ = std::fs::remove_dir_all(&staging);
            bail!("tarball did not contain {name}/ontology.toml");
        }
        std::fs::write(staging.join(".digest"), &entry.digest)?;
        let _ = std::fs::remove_dir_all(&version_dir);
        std::fs::rename(&staging, &version_dir).with_context(|| {
            format!(
                "cannot move verified cache entry into {}",
                version_dir.display()
            )
        })?;
        Ok(entry_dir)
    }

    async fn download(&self, url: &str) -> Result<Vec<u8>> {
        let resp = self.request(url).send().await?;
        if resp.status().is_success() {
            return Ok(resp.bytes().await?.to_vec());
        }
        // Private-repo release assets 404 on their browser URL even with a
        // token; resolve them through the GitHub API instead.
        if let (Some(token), Some(api)) = (self.token.as_deref(), asset_api_lookup(url)) {
            return download_via_github_api(&self.client, token, &api).await;
        }
        bail!("download failed ({}): {url}", resp.status());
    }
}

struct AssetLookup {
    release_url: String,
    filename: String,
}

/// `...github.com/{owner}/{repo}/releases/download/{tag}/{file}` -> API lookup.
fn asset_api_lookup(url: &str) -> Option<AssetLookup> {
    let rest = url.strip_prefix("https://github.com/")?;
    let mut parts = rest.split('/');
    let owner = parts.next()?;
    let repo = parts.next()?;
    if (parts.next()?, parts.next()?) != ("releases", "download") {
        return None;
    }
    let tag = parts.next()?;
    let filename = parts.next()?.to_string();
    Some(AssetLookup {
        release_url: format!("https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}"),
        filename,
    })
}

async fn download_via_github_api(
    client: &reqwest::Client,
    token: &str,
    lookup: &AssetLookup,
) -> Result<Vec<u8>> {
    #[derive(Deserialize)]
    struct Release {
        assets: Vec<Asset>,
    }
    #[derive(Deserialize)]
    struct Asset {
        name: String,
        url: String,
    }
    let release: Release = client
        .get(&lookup.release_url)
        .header("User-Agent", "il-cli")
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?
        .error_for_status()
        .context("GitHub release lookup failed")?
        .json()
        .await?;
    let asset = release
        .assets
        .iter()
        .find(|a| a.name == lookup.filename)
        .ok_or_else(|| anyhow!("release has no asset named {}", lookup.filename))?;
    let bytes = client
        .get(&asset.url)
        .header("User-Agent", "il-cli")
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/octet-stream")
        .send()
        .await?
        .error_for_status()
        .context("GitHub asset download failed")?
        .bytes()
        .await?;
    Ok(bytes.to_vec())
}

pub fn read_manifest(entry_dir: &Path) -> Result<Manifest> {
    let path = entry_dir.join("ontology.toml");
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("cannot read {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("invalid manifest: {}", path.display()))
}

pub fn cache_dir() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .ok_or_else(|| anyhow!("cannot determine home directory"))?;
    Ok(PathBuf::from(home).join(".inputlayer").join("ontologies"))
}

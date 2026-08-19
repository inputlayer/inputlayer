//! Registry client: index fetch, version resolution, digest-verified download,
//! and the local ontology cache.
//!
//! The registry is Helm-shaped (see inputlayer/ontology-registry): `index.json`
//! maps ontology names to versions, each carrying a tarball URL and a sha256
//! digest. Nothing is unpacked or deployed unless the digest matches — a
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

impl Registry {
    pub fn new(index_url: String, token: Option<String>) -> Self {
        Self {
            index_url,
            token,
            client: reqwest::Client::new(),
        }
    }

    fn request(&self, url: &str) -> reqwest::RequestBuilder {
        let mut req = self.client.get(url).header("User-Agent", "il-cli");
        if let Some(token) = &self.token {
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
                "registry index fetch failed ({}): {} — private registries need IL_REGISTRY_TOKEN",
                resp.status(),
                self.index_url
            );
        }
        resp.json().await.context("registry index is not valid JSON")
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
        Ok((name.to_string(), entry.clone()))
    }

    /// Fetch into the cache (digest-verified) and return the unpacked entry dir.
    /// A cached entry is trusted because it can only have been written by a
    /// successful verified fetch.
    pub async fn fetch(&self, name: &str, entry: &IndexEntry) -> Result<PathBuf> {
        let dir = cache_dir()?.join(name).join(&entry.version);
        let entry_dir = dir.join(name);
        if entry_dir.join("ontology.toml").is_file() {
            return Ok(entry_dir);
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

        std::fs::create_dir_all(&dir)
            .with_context(|| format!("cannot create cache dir {}", dir.display()))?;
        let decoder = flate2::read::GzDecoder::new(std::io::Cursor::new(bytes));
        tar::Archive::new(decoder)
            .unpack(&dir)
            .context("failed to unpack ontology tarball")?;
        if !entry_dir.join("ontology.toml").is_file() {
            bail!("tarball did not contain {name}/ontology.toml");
        }
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

/// `…github.com/{owner}/{repo}/releases/download/{tag}/{file}` → API lookup.
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

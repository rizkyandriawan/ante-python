//! Shared security primitives for Ante's network adapters.
//!
//! A loaded [`Security`] owns the TLS server configuration and the set of
//! users. Passwords are Argon2id PHC strings. Queue access is default-deny and
//! exact-match: a principal may only touch queue names explicitly granted to
//! it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use argon2::password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, SaltString};
use argon2::{Argon2, PasswordVerifier};
pub use rustls::pki_types::ServerName;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncWrite};
pub use tokio_rustls::{TlsAcceptor, TlsConnector};

const MAX_USERNAME: usize = 64;
const MAX_QUEUE: usize = 255;

/// A valid Argon2id hash used to keep unknown-user authentication as expensive
/// as a wrong password. The corresponding password is not a credential: no
/// principal is returned even if it verifies.
const DUMMY_PASSWORD_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$jjQSJ8TGFtpbU3va3iRzQQ$V15wZO6eeQOplzE3pqLl9qxLgtxuZ4hDl0ytdsClU4k";

#[derive(Debug)]
pub enum SecurityError {
    Io(std::io::Error),
    Json(serde_json::Error),
    Invalid(String),
    Tls(String),
    Password(String),
}

impl fmt::Display for SecurityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecurityError::Io(error) => write!(f, "{error}"),
            SecurityError::Json(error) => write!(f, "{error}"),
            SecurityError::Invalid(message) => write!(f, "{message}"),
            SecurityError::Tls(message) => write!(f, "{message}"),
            SecurityError::Password(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for SecurityError {}

impl From<std::io::Error> for SecurityError {
    fn from(error: std::io::Error) -> Self {
        SecurityError::Io(error)
    }
}

impl From<serde_json::Error> for SecurityError {
    fn from(error: serde_json::Error) -> Self {
        SecurityError::Json(error)
    }
}

pub type Result<T> = std::result::Result<T, SecurityError>;

/// A queue-scoped permission. Unknown strings are rejected by serde.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Permission {
    Publish,
    Consume,
    Inspect,
    Declare,
    Purge,
    Delete,
}

impl Permission {
    pub fn as_str(self) -> &'static str {
        match self {
            Permission::Publish => "publish",
            Permission::Consume => "consume",
            Permission::Inspect => "inspect",
            Permission::Declare => "declare",
            Permission::Purge => "purge",
            Permission::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Grants {
    queues: BTreeMap<Permission, BTreeSet<String>>,
}

impl Grants {
    pub fn insert(&mut self, permission: Permission, queue: impl Into<String>) -> Result<()> {
        let queue = queue.into();
        validate_queue(&queue)?;
        self.queues.entry(permission).or_default().insert(queue);
        Ok(())
    }

    pub fn allows(&self, permission: Permission, queue: &str) -> bool {
        self.queues
            .get(&permission)
            .is_some_and(|queues| queues.contains(queue))
    }

    pub fn queues(&self, permission: Permission) -> impl Iterator<Item = &str> {
        self.queues
            .get(&permission)
            .into_iter()
            .flat_map(|queues| queues.iter().map(String::as_str))
    }

    pub fn is_empty(&self) -> bool {
        self.queues.values().all(BTreeSet::is_empty)
    }
}

#[derive(Debug, Clone)]
pub struct Principal {
    pub username: String,
    grants: Grants,
}

impl Principal {
    pub fn new(username: impl Into<String>, grants: Grants) -> Self {
        Principal {
            username: username.into(),
            grants,
        }
    }

    pub fn allows(&self, permission: Permission, queue: &str) -> bool {
        self.grants.allows(permission, queue)
    }

    pub fn grants(&self) -> &Grants {
        &self.grants
    }
}

#[derive(Debug)]
struct User {
    password_hash: String,
    principal: Principal,
}

/// Authenticated TLS/credential configuration.
#[derive(Debug)]
pub struct Security {
    users: BTreeMap<String, User>,
    tls: Arc<ServerConfig>,
}

impl Security {
    /// Load and validate a JSON security file before any listener is bound.
    ///
    /// Relative certificate paths are resolved against the security file's
    /// directory. A configuration with users but no TLS is rejected.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let bytes = std::fs::read(path)?;
        let file: FileConfig = serde_json::from_slice(&bytes)?;
        if file.users.is_empty() {
            return Err(SecurityError::Invalid(
                "security file must define at least one user".into(),
            ));
        }
        let tls = file.tls.ok_or_else(|| {
            SecurityError::Invalid("security file requires a tls certificate and key".into())
        })?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        let cert = resolve(base, &tls.cert);
        let key = resolve(base, &tls.key);
        let tls = server_config(&cert, &key)?;

        let mut users = BTreeMap::new();
        for (name, raw) in file.users {
            validate_username(&name)?;
            let parsed = PasswordHash::new(&raw.password_hash).map_err(|error| {
                SecurityError::Password(format!("invalid hash for {name}: {error}"))
            })?;
            if parsed.algorithm.as_str() != "argon2id" {
                return Err(SecurityError::Password(format!(
                    "user {name} must use an argon2id password hash"
                )));
            }
            let mut grants = Grants::default();
            for (permission, queues) in raw.permissions {
                for queue in queues {
                    grants.insert(permission, queue)?;
                }
            }
            if grants.is_empty() {
                return Err(SecurityError::Invalid(format!(
                    "user {name} has no queue permissions"
                )));
            }
            users.insert(
                name.clone(),
                User {
                    password_hash: raw.password_hash,
                    principal: Principal::new(name, grants),
                },
            );
        }
        Ok(Security { users, tls })
    }

    /// Verify a password and return the user's principal.
    ///
    /// The error is intentionally indistinguishable between unknown users and
    /// wrong passwords at the protocol boundary.
    pub fn authenticate(&self, username: &str, password: &str) -> Option<Principal> {
        let (hash, principal) = match self.users.get(username) {
            Some(user) => (user.password_hash.as_str(), Some(&user.principal)),
            None => (DUMMY_PASSWORD_HASH, None),
        };
        verify_password(hash, password)
            .then(|| principal.cloned())
            .flatten()
    }

    pub fn tls_config(&self) -> Arc<ServerConfig> {
        self.tls.clone()
    }

    pub fn users(&self) -> impl Iterator<Item = &str> {
        self.users.keys().map(String::as_str)
    }

    pub fn allows(
        &self,
        principal: Option<&Principal>,
        permission: Permission,
        queue: &str,
    ) -> bool {
        principal.is_some_and(|principal| principal.allows(permission, queue))
    }
}

/// Hash a password as an Argon2id PHC string.
pub fn hash_password(password: &str) -> Result<String> {
    if password.is_empty() {
        return Err(SecurityError::Password("password must not be empty".into()));
    }
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|error| SecurityError::Password(error.to_string()))
}

pub fn verify_password(hash: &str, password: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

/// Build a TLS server configuration from PEM certificate and key files.
pub fn server_config(cert: &Path, key: &Path) -> Result<Arc<ServerConfig>> {
    let certs = read_certs(cert)?;
    if certs.is_empty() {
        return Err(SecurityError::Tls(format!(
            "no certificates found in {}",
            cert.display()
        )));
    }
    let key = read_key(key)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|error| SecurityError::Tls(format!("invalid TLS certificate/key: {error}")))?;
    Ok(Arc::new(config))
}

/// Build a TLS client configuration trusting the PEM certificates in `ca`.
pub fn client_config(ca: &Path) -> Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    for cert in read_certs(ca)? {
        roots
            .add(cert)
            .map_err(|error| SecurityError::Tls(format!("invalid CA certificate: {error}")))?;
    }
    if roots.is_empty() {
        return Err(SecurityError::Tls(format!(
            "no CA certificates found in {}",
            ca.display()
        )));
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

fn read_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(SecurityError::Io)
}

fn read_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path)?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(SecurityError::Io)?
        .ok_or_else(|| SecurityError::Tls(format!("no private key found in {}", path.display())))
}

fn resolve(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn validate_username(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_USERNAME {
        return Err(SecurityError::Invalid(format!(
            "username must be 1..={MAX_USERNAME} bytes"
        )));
    }
    if name.chars().any(|ch| ch.is_control()) {
        return Err(SecurityError::Invalid(
            "username must not contain control characters".into(),
        ));
    }
    Ok(())
}

fn validate_queue(queue: &str) -> Result<()> {
    if queue.is_empty() || queue.len() > MAX_QUEUE {
        return Err(SecurityError::Invalid(format!(
            "queue permission must be 1..={MAX_QUEUE} bytes"
        )));
    }
    if queue.chars().any(|ch| ch.is_control()) {
        return Err(SecurityError::Invalid(
            "queue permission must not contain control characters".into(),
        ));
    }
    if queue.contains(['*', '#']) {
        return Err(SecurityError::Invalid(
            "queue permissions must be exact names, not wildcards".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    tls: Option<TlsFiles>,
    users: BTreeMap<String, UserFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TlsFiles {
    cert: PathBuf,
    key: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UserFile {
    password_hash: String,
    permissions: BTreeMap<Permission, Vec<String>>,
}

/// A stream that can be either plain TCP or TLS.
pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

pub type BoxedStream = Box<dyn AsyncStream>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_and_verifies_passwords() {
        let hash = hash_password("correct horse").unwrap();
        assert!(hash.starts_with("$argon2id$"));
        assert!(verify_password(&hash, "correct horse"));
        assert!(!verify_password(&hash, "wrong"));
        assert!(hash_password("").is_err());
    }

    #[test]
    fn grants_are_exact_and_default_deny() {
        let mut grants = Grants::default();
        grants.insert(Permission::Publish, "jobs").unwrap();
        let principal = Principal::new("producer", grants);
        assert!(principal.allows(Permission::Publish, "jobs"));
        assert!(!principal.allows(Permission::Publish, "jobs.other"));
        assert!(!principal.allows(Permission::Consume, "jobs"));
        assert!(grants_allows_wildcard());
    }

    fn grants_allows_wildcard() -> bool {
        let mut grants = Grants::default();
        grants.insert(Permission::Publish, "jobs.*").is_err()
    }

    #[test]
    fn rejects_bad_names() {
        let mut grants = Grants::default();
        assert!(grants.insert(Permission::Publish, "").is_err());
        assert!(grants.insert(Permission::Publish, "a\nb").is_err());
        assert!(grants.insert(Permission::Publish, "a.*").is_err());
    }
}

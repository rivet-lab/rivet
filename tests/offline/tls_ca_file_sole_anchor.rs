//! `tls.ca_file` is the only trust anchor: a server certificate signed by another CA is refused.
//! Each engine's `rivet init` dials a local TLS listener; the oracle is the listener, which
//! records whether the client sent anything after the handshake.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use openssl::asn1::Asn1Time;
use openssl::bn::{BigNum, MsbOption};
use openssl::ec::{EcGroup, EcKey};
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::{PKey, Private};
use openssl::ssl::{SslAcceptor, SslMethod};
use openssl::x509::extension::{
    BasicConstraints, ExtendedKeyUsage, KeyUsage, SubjectAlternativeName,
};
use openssl::x509::{X509, X509NameBuilder};

struct Cert {
    x509: X509,
    key: PKey<Private>,
}

fn new_key() -> PKey<Private> {
    let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
    PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap()
}

/// A CA (`issuer` None, self-signed) or a loopback server certificate issued by `issuer`.
fn issue(cn: &str, issuer: Option<&Cert>) -> Cert {
    let key = new_key();
    let mut name = X509NameBuilder::new().unwrap();
    name.append_entry_by_nid(Nid::COMMONNAME, cn).unwrap();
    let name = name.build();
    let mut b = X509::builder().unwrap();
    b.set_version(2).unwrap();
    let mut serial = BigNum::new().unwrap();
    serial.rand(64, MsbOption::MAYBE_ZERO, false).unwrap();
    b.set_serial_number(&serial.to_asn1_integer().unwrap())
        .unwrap();
    b.set_subject_name(&name).unwrap();
    b.set_issuer_name(issuer.map_or(&name, |i| i.x509.subject_name()))
        .unwrap();
    b.set_pubkey(&key).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    b.set_not_before(&Asn1Time::from_unix(now - 3600).unwrap())
        .unwrap();
    b.set_not_after(&Asn1Time::from_unix(now + 86_400).unwrap())
        .unwrap();
    match issuer {
        None => {
            b.append_extension(BasicConstraints::new().critical().ca().build().unwrap())
                .unwrap();
            b.append_extension(
                KeyUsage::new()
                    .critical()
                    .key_cert_sign()
                    .crl_sign()
                    .build()
                    .unwrap(),
            )
            .unwrap();
        }
        Some(i) => {
            let san = {
                let ctx = b.x509v3_context(Some(&i.x509), None);
                SubjectAlternativeName::new()
                    .ip("127.0.0.1")
                    .dns("localhost")
                    .build(&ctx)
                    .unwrap()
            };
            b.append_extension(san).unwrap();
            b.append_extension(ExtendedKeyUsage::new().server_auth().build().unwrap())
                .unwrap();
        }
    }
    b.sign(issuer.map_or(&key, |i| &i.key), MessageDigest::sha256())
        .unwrap();
    Cert {
        x509: b.build(),
        key,
    }
}

fn write_pem(dir: &Path, file: &str, cert: &Cert) -> PathBuf {
    let path = dir.join(file);
    std::fs::write(&path, cert.x509.to_pem().unwrap()).unwrap();
    path
}

#[derive(Clone, Copy, Debug)]
enum Engine {
    Postgres,
    Mysql,
    Mssql,
    Mongo,
}

impl Engine {
    fn url(self, port: u16) -> String {
        match self {
            Engine::Postgres => format!("postgresql://u:p@127.0.0.1:{port}/db"),
            Engine::Mysql => format!("mysql://u:p@127.0.0.1:{port}/db"),
            Engine::Mssql => format!("sqlserver://u:p@127.0.0.1:{port}/db"),
            Engine::Mongo => {
                format!("mongodb://127.0.0.1:{port}/db?serverSelectionTimeoutMS=3000")
            }
        }
    }
}

/// TDS framing around the TLS handshake bytes, dropped once the handshake is done.
struct TdsWrapped {
    inner: TcpStream,
    left: usize,
    passthrough: bool,
}

impl Read for TdsWrapped {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.passthrough {
            return self.inner.read(buf);
        }
        if self.left == 0 {
            let mut head = [0u8; 8];
            self.inner.read_exact(&mut head)?;
            self.left = (u16::from_be_bytes([head[2], head[3]]) as usize).saturating_sub(8);
        }
        let want = buf.len().min(self.left);
        let n = self.inner.read(&mut buf[..want])?;
        self.left -= n;
        Ok(n)
    }
}

impl Write for TdsWrapped {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.passthrough {
            return self.inner.write(buf);
        }
        let chunk = &buf[..buf.len().min(4000)];
        let total = (chunk.len() + 8) as u16;
        let mut out = vec![0x12, 0x01];
        out.extend_from_slice(&total.to_be_bytes());
        out.extend_from_slice(&[0, 0, 1, 0]);
        out.extend_from_slice(chunk);
        self.inner.write_all(&out)?;
        Ok(chunk.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn mysql_greeting() -> Vec<u8> {
    let mut p = vec![0x0a];
    p.extend_from_slice(b"8.0.0\0");
    p.extend_from_slice(&1u32.to_le_bytes());
    p.extend_from_slice(b"abcdefgh");
    p.push(0);
    p.extend_from_slice(&0xffffu16.to_le_bytes());
    p.push(0x21);
    p.extend_from_slice(&2u16.to_le_bytes());
    p.extend_from_slice(&0x000fu16.to_le_bytes());
    p.push(21);
    p.extend_from_slice(&[0u8; 10]);
    p.extend_from_slice(b"ijklmnopqrst\0");
    p.extend_from_slice(b"mysql_native_password\0");
    let mut out = (p.len() as u32).to_le_bytes()[..3].to_vec();
    out.push(0);
    out.extend_from_slice(&p);
    out
}

fn tds_prelogin_reply() -> Vec<u8> {
    let payload: Vec<u8> = vec![
        0x00, 0, 11, 0, 6, 0x01, 0, 17, 0, 1, 0xff, 0, 0, 0, 0, 0, 0, 0x01,
    ];
    let total = (payload.len() + 8) as u16;
    let mut out = vec![0x04, 0x01];
    out.extend_from_slice(&total.to_be_bytes());
    out.extend_from_slice(&[0, 0, 1, 0]);
    out.extend_from_slice(&payload);
    out
}

fn handle(engine: Engine, mut tcp: TcpStream, acceptor: &SslAcceptor, trusted: &AtomicBool) {
    tcp.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut buf = [0u8; 4096];
    let after = |mut s: Box<dyn Read>| {
        let mut one = [0u8; 1];
        if s.read(&mut one).map(|n| n > 0).unwrap_or(false) {
            trusted.store(true, Ordering::SeqCst);
        }
    };
    match engine {
        Engine::Postgres => {
            let mut req = [0u8; 8];
            if tcp.read_exact(&mut req).is_err() || tcp.write_all(b"S").is_err() {
                return;
            }
            if let Ok(s) = acceptor.accept(tcp) {
                after(Box::new(s));
            }
        }
        Engine::Mysql => {
            if tcp.write_all(&mysql_greeting()).is_err() {
                return;
            }
            let mut ssl_request = [0u8; 36];
            if tcp.read_exact(&mut ssl_request).is_err() {
                return;
            }
            if let Ok(s) = acceptor.accept(tcp) {
                after(Box::new(s));
            }
        }
        Engine::Mssql => {
            if tcp.read(&mut buf).unwrap_or(0) == 0 || tcp.write_all(&tds_prelogin_reply()).is_err()
            {
                return;
            }
            let wrapped = TdsWrapped {
                inner: tcp,
                left: 0,
                passthrough: false,
            };
            if let Ok(mut s) = acceptor.accept(wrapped) {
                s.get_mut().passthrough = true;
                after(Box::new(s));
            }
        }
        Engine::Mongo => {
            if let Ok(s) = acceptor.accept(tcp) {
                after(Box::new(s));
            }
        }
    }
}

struct Listener {
    port: u16,
    trusted: Arc<AtomicBool>,
}

fn listen(engine: Engine, server: &Cert) -> Listener {
    let mut a = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
    a.set_private_key(&server.key).unwrap();
    a.set_certificate(&server.x509).unwrap();
    let acceptor = Arc::new(a.build());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let trusted = Arc::new(AtomicBool::new(false));
    let seen = trusted.clone();
    std::thread::spawn(move || {
        for conn in listener.incoming().flatten() {
            let acceptor = acceptor.clone();
            let seen = seen.clone();
            std::thread::spawn(move || handle(engine, conn, &acceptor, &seen));
        }
    });
    Listener { port, trusted }
}

/// Whether the client of `rivet init --tls <mode> [--tls-ca ca]` sent data after the handshake, with its stderr.
fn client_trusted(
    engine: Engine,
    server: &Cert,
    mode: &str,
    ca: Option<&Path>,
    system_roots: Option<&Path>,
) -> (bool, String) {
    let l = listen(engine, server);
    let cwd = tempfile::tempdir().unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rivet"));
    cmd.current_dir(cwd.path())
        .args(["init", "--source", &engine.url(l.port), "--tls", mode]);
    if let Some(ca) = ca {
        cmd.arg("--tls-ca").arg(ca);
    }
    if let Some(roots) = system_roots {
        cmd.env("SSL_CERT_FILE", roots);
    }
    let mut child = cmd
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rivet");
    let start = Instant::now();
    while child.try_wait().unwrap().is_none() {
        if start.elapsed() > Duration::from_secs(90) {
            child.kill().ok();
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let mut said = String::new();
    if let Some(mut err) = child.stderr.take() {
        err.read_to_string(&mut said).ok();
    }
    child.wait().ok();
    (l.trusted.load(Ordering::SeqCst), said)
}

struct Pki {
    dir: tempfile::TempDir,
    ca_a: Cert,
    ca_b: Cert,
}

fn pki() -> Pki {
    Pki {
        dir: tempfile::tempdir().unwrap(),
        ca_a: issue("rivet test CA A", None),
        ca_b: issue("rivet test CA B", None),
    }
}

fn wrong_ca_is_refused(engine: Engine) {
    let p = pki();
    let ca_a = write_pem(p.dir.path(), "a.pem", &p.ca_a);
    let by_b = issue("localhost", Some(&p.ca_b));
    for mode in ["verify-ca", "verify-full"] {
        let (trusted, said) = client_trusted(engine, &by_b, mode, Some(&ca_a), None);
        assert!(
            !trusted,
            "{engine:?} {mode}: a certificate signed by a CA other than ca_file must be refused:\n{said}"
        );
    }
}

fn right_ca_is_accepted(engine: Engine) {
    let p = pki();
    let ca_a = write_pem(p.dir.path(), "a.pem", &p.ca_a);
    let by_a = issue("localhost", Some(&p.ca_a));
    for mode in ["verify-ca", "verify-full"] {
        let (trusted, said) = client_trusted(engine, &by_a, mode, Some(&ca_a), None);
        assert!(
            trusted,
            "{engine:?} {mode}: a certificate signed by the ca_file CA must be accepted:\n{said}"
        );
    }
}

#[cfg(target_os = "linux")]
fn system_store_is_not_a_second_anchor(engine: Engine) {
    let p = pki();
    let ca_a = write_pem(p.dir.path(), "a.pem", &p.ca_a);
    let ca_b = write_pem(p.dir.path(), "b.pem", &p.ca_b);
    let by_b = issue("localhost", Some(&p.ca_b));
    let (trusted, said) = client_trusted(engine, &by_b, "verify-ca", Some(&ca_a), Some(&ca_b));
    assert!(
        !trusted,
        "{engine:?}: a certificate the system store trusts must be refused when ca_file names another CA:\n{said}"
    );
}

#[cfg(target_os = "linux")]
fn system_store_still_decides_without_a_ca_file(engine: Engine) {
    let p = pki();
    let ca_b = write_pem(p.dir.path(), "b.pem", &p.ca_b);
    let by_b = issue("localhost", Some(&p.ca_b));
    let (trusted, said) = client_trusted(engine, &by_b, "verify-full", None, Some(&ca_b));
    assert!(
        trusted,
        "{engine:?}: with no ca_file the system store keeps deciding:\n{said}"
    );
}

#[test]
fn postgres_wrong_ca_is_refused() {
    wrong_ca_is_refused(Engine::Postgres);
}

#[test]
fn postgres_right_ca_is_accepted() {
    right_ca_is_accepted(Engine::Postgres);
}

#[cfg(target_os = "linux")]
#[test]
fn postgres_system_store_is_not_a_second_anchor() {
    system_store_is_not_a_second_anchor(Engine::Postgres);
}

#[cfg(target_os = "linux")]
#[test]
fn postgres_system_store_still_decides_without_a_ca_file() {
    system_store_still_decides_without_a_ca_file(Engine::Postgres);
}

#[test]
fn mysql_wrong_ca_is_refused() {
    wrong_ca_is_refused(Engine::Mysql);
}

#[test]
fn mysql_right_ca_is_accepted() {
    right_ca_is_accepted(Engine::Mysql);
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "open defect #789, 2026-11-06: the mysql driver trusts the system store beside ca_file and has no sole-anchor option; closes with a driver patch"]
fn mysql_system_store_is_not_a_second_anchor() {
    system_store_is_not_a_second_anchor(Engine::Mysql);
}

#[cfg(target_os = "linux")]
#[test]
fn mysql_system_store_still_decides_without_a_ca_file() {
    system_store_still_decides_without_a_ca_file(Engine::Mysql);
}

#[test]
fn mssql_wrong_ca_is_refused() {
    wrong_ca_is_refused(Engine::Mssql);
}

#[test]
fn mssql_right_ca_is_accepted() {
    right_ca_is_accepted(Engine::Mssql);
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "open defect #789, 2026-11-06: the tiberius driver trusts the system store beside ca_file and has no sole-anchor option; closes with a driver patch"]
fn mssql_system_store_is_not_a_second_anchor() {
    system_store_is_not_a_second_anchor(Engine::Mssql);
}

#[cfg(target_os = "linux")]
#[test]
fn mssql_system_store_still_decides_without_a_ca_file() {
    system_store_still_decides_without_a_ca_file(Engine::Mssql);
}

#[test]
fn mongo_wrong_ca_is_refused() {
    wrong_ca_is_refused(Engine::Mongo);
}

#[test]
fn mongo_right_ca_is_accepted() {
    right_ca_is_accepted(Engine::Mongo);
}

#[cfg(target_os = "linux")]
#[test]
fn mongo_system_store_is_not_a_second_anchor() {
    system_store_is_not_a_second_anchor(Engine::Mongo);
}

#![allow(dead_code)]
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose,
};
use rustls::{ClientConfig, RootCertStore};
use std::{path::Path, sync::Arc, time::Duration};
use syslog_gateway::config::TlsSettings;

pub struct Ca {
    pub cert: Certificate,
    key: KeyPair,
    params: CertificateParams,
}
pub struct Leaf {
    pub cert: Certificate,
    pub key: KeyPair,
}
impl Ca {
    pub fn new() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages =
            vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::DigitalSignature, KeyUsagePurpose::CrlSign];
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        Self { cert, key, params }
    }
    pub fn leaf(&self, client: bool) -> Leaf {
        self.dated_leaf(client, 2020, 2099)
    }
    pub fn dated_leaf(&self, client: bool, from: i32, until: i32) -> Leaf {
        let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        params.not_before = rcgen::date_time_ymd(from, 1, 1);
        params.not_after = rcgen::date_time_ymd(until, 1, 1);
        params.extended_key_usages =
            vec![if client { ExtendedKeyUsagePurpose::ClientAuth } else { ExtendedKeyUsagePurpose::ServerAuth }];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &Issuer::from_params(&self.params, &self.key)).unwrap();
        Leaf { cert, key }
    }
    pub fn client(&self, identity: Option<&Leaf>) -> Arc<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(self.cert.der().clone()).unwrap();
        let builder = ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots);
        Arc::new(match identity {
            Some(leaf) => builder
                .with_client_auth_cert(
                    vec![leaf.cert.der().clone()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(leaf.key.serialize_der()).into(),
                )
                .unwrap(),
            None => builder.with_no_client_auth(),
        })
    }
    pub fn files(&self, dir: &Path, leaf: &Leaf, mtls: bool) -> TlsSettings {
        let settings = TlsSettings {
            cert_file: dir.join("server.pem"),
            key_file: dir.join("server.key"),
            client_ca_file: mtls.then(|| dir.join("ca.pem")),
            reload_interval: Duration::from_secs(1),
            handshake_timeout: Duration::from_secs(1),
        };
        std::fs::write(&settings.cert_file, leaf.cert.pem()).unwrap();
        std::fs::write(&settings.key_file, leaf.key.serialize_pem()).unwrap();
        if let Some(path) = &settings.client_ca_file {
            std::fs::write(path, self.cert.pem()).unwrap();
        }
        settings
    }
}
pub fn env(settings: &TlsSettings) -> Vec<(&'static str, String)> {
    let mut values = vec![
        ("SYSLOG_TLS_ADDR", "127.0.0.1:0".into()),
        ("SYSLOG_TLS_CERT_FILE", settings.cert_file.to_string_lossy().into_owned()),
        ("SYSLOG_TLS_KEY_FILE", settings.key_file.to_string_lossy().into_owned()),
        ("SYSLOG_TLS_RELOAD_SECS", "1".into()),
        ("SYSLOG_TLS_HANDSHAKE_SECS", "1".into()),
        ("SYSLOG_MAX_CONNECTIONS", "4".into()),
        ("SYSLOG_IDLE_SECS", "30".into()),
    ];
    if let Some(path) = &settings.client_ca_file {
        values.push(("SYSLOG_TLS_CLIENT_CA_FILE", path.to_string_lossy().into_owned()));
    }
    values
}

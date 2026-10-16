use crate::{
    config::{safe_name, TokenSource, MAX_ENTRIES},
    error::{Error, Result},
    model::Target,
};
use contracts::cronet::cmdb::v1::{self as pb, cmdb_client::CmdbClient};
use secrecy::ExposeSecret;
use std::{collections::HashSet, time::Duration};
use tonic::{
    transport::{ClientTlsConfig, Endpoint},
    Request,
};

#[derive(Default, Debug)]
pub struct Diagnostics {
    pub unplaced: usize,
    pub missing_ip: usize,
    pub ambiguous_ip: usize,
    pub invalid_site: usize,
}
pub fn normalize(rows: Vec<pb::PollTarget>) -> Result<(Vec<Target>, Diagnostics)> {
    let mut out = Vec::new();
    let mut diagnostics = Diagnostics::default();
    let mut seen = HashSet::new();
    for row in rows {
        let device_id = row.device_id.parse().map_err(|_| Error::Cmdb)?;
        if !seen.insert(device_id) {
            return Err(Error::Cmdb);
        }
        let (Some(_site_id), Some(site)) = (row.site_id, row.site_name) else {
            diagnostics.unplaced += 1;
            continue;
        };
        if !safe_name(&site) {
            diagnostics.invalid_site += 1;
            continue;
        }
        match row.management_ips.len() {
            0 => {
                diagnostics.missing_ip += 1;
                continue;
            }
            1 => {}
            _ => {
                diagnostics.ambiguous_ip += 1;
                continue;
            }
        }
        out.push(Target { device_id, site, ip: row.management_ips[0].parse().map_err(|_| Error::Cmdb)? });
    }
    Ok((out, diagnostics))
}
#[async_trait::async_trait]
pub trait CmdbSource: Send + Sync {
    async fn fetch(&self) -> Result<Vec<pb::PollTarget>>;
}
pub struct TonicCmdbSource {
    pub endpoint: String,
    pub token: TokenSource,
}
#[async_trait::async_trait]
impl CmdbSource for TonicCmdbSource {
    async fn fetch(&self) -> Result<Vec<pb::PollTarget>> {
        // Reload a rotated file once per complete resync, so all pages use one token.
        let token = self.token.load().map_err(|_| Error::Cmdb)?;
        let mut endpoint = Endpoint::from_shared(self.endpoint.clone()).map_err(|_| Error::Cmdb)?;
        if endpoint.uri().authority().is_some_and(|a| a.as_str().contains('@')) {
            return Err(Error::Cmdb);
        }
        if endpoint.uri().scheme_str() == Some("https") {
            endpoint = endpoint.tls_config(ClientTlsConfig::new().with_webpki_roots()).map_err(|_| Error::Cmdb)?;
        }
        let channel = endpoint
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(5))
            .connect()
            .await
            .map_err(|_| Error::Cmdb)?;
        let mut client = CmdbClient::new(channel);
        let mut cursor: Option<String> = None;
        let mut out = Vec::new();
        loop {
            let mut request =
                Request::new(pb::ListPollTargetsRequest { limit: Some(200), after_device_id: cursor.clone() });
            let mut bearer: tonic::metadata::MetadataValue<_> =
                format!("Bearer {}", token.expose_secret()).parse().map_err(|_| Error::Cmdb)?;
            bearer.set_sensitive(true);
            request.metadata_mut().insert("authorization", bearer);
            let response = client.list_poll_targets(request).await.map_err(|_| Error::Cmdb)?.into_inner();
            out.extend(response.targets);
            if out.len() > MAX_ENTRIES {
                return Err(Error::Cmdb);
            }
            let Some(next) = response.next_after_device_id else { break };
            if cursor.as_ref().is_some_and(|old| next <= *old) {
                return Err(Error::Cmdb);
            }
            cursor = Some(next);
        }
        Ok(out)
    }
}

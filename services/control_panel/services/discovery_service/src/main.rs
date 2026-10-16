use clap::Parser;
use discovery_service::{
    cmdb::TonicCmdbClient,
    config::{self, Cli, Command, RuntimeConfig},
    error::{Error, Result},
    probe::{Snmp2Probe, SurgeReachability},
    scan,
};
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt().json().with_writer(std::io::stderr).with_env_filter("discovery_service=info").init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error=%error,"Discovery scan failed");
            ExitCode::FAILURE
        }
    }
}
async fn run() -> Result<()> {
    let Cli { command: Command::Scan(args) } = Cli::parse();
    let runtime = RuntimeConfig::from_env()?;
    let plan = args.plan()?;
    let store = config::credential_store()?;
    let credential = tokio::time::timeout(std::time::Duration::from_secs(10), store.get(&args.credential_ref))
        .await
        .map_err(|_| Error::Credential)?
        .map_err(|_| Error::Credential)?;
    let snmp = Snmp2Probe::new(credential, args.snmp_version, runtime.allow_legacy_crypto)?;
    let cmdb = TonicCmdbClient::connect(runtime.cmdb_endpoint, runtime.token, runtime.cmdb_timeout).await?;
    let icmp = if args.icmp { SurgeReachability::new(plan.is_ipv6()) } else { SurgeReachability::disabled() };
    let report = scan::scan(&plan, &snmp, &icmp, &cmdb).await?;
    let output =
        if args.json_results { serde_json::to_string(&report) } else { serde_json::to_string(&report.summary) }
            .map_err(|_| Error::Config)?;
    println!("{output}");
    Ok(())
}

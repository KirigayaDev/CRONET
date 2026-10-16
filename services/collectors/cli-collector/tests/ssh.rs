#![allow(clippy::unwrap_used)]
mod support;
use cli_collector::{session::collect, transport::KnownHosts};
use contracts::netmon::v1::raw_cli_result::Status;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use support::*;

#[tokio::test]
async fn real_ssh_password_known_key_fragmented_prompt_two_commands_one_shell() {
    let s = server().await;
    let result = collect(&task(&s), &policy(&s), &store()).await;
    assert_eq!(result.status, Status::Ok as i32, "{result:?}");
    assert_eq!(result.outputs.len(), 2);
    let first = &result.outputs[0];
    assert!(first.raw_output.windows(5).any(|b| b == b"\x1b[32m"));
    assert!(first.normalized_output.as_ref().unwrap().contains("ОС device 1.0"));
    assert!(!first.normalized_output.as_ref().unwrap().contains('\x1b'));
    assert!(result.outputs[1].normalized_output.as_ref().unwrap().contains("eth0 up"));
    assert_eq!(s.stats.connections.load(Ordering::SeqCst), 1);
    assert_eq!(s.stats.shells.load(Ordering::SeqCst), 1);
    assert_eq!(s.stats.commands.load(Ordering::SeqCst), 2);
    assert!(!format!("{result:?}").contains(PASSWORD));
}
#[tokio::test]
async fn wrong_password_is_auth_fail() {
    let s = server().await;
    let r = collect(&task(&s), &policy(&s), &Store { password: "wrong", fail: false }).await;
    assert_eq!(r.status, Status::AuthFail as i32);
    assert_eq!(s.stats.shells.load(Ordering::SeqCst), 0);
    assert!(!r.error_detail.contains("wrong"));
}
#[tokio::test]
async fn unknown_or_changed_host_key_fails_before_authentication() {
    let s = server().await;
    let other = server().await;
    let mut p = policy(&s);
    p.hosts = Arc::new(
        KnownHosts::parse(&other.known_hosts.replace(
            &other.target.to_string().replace("127.0.0.1:", "[127.0.0.1]:"),
            &s.target.to_string().replace("127.0.0.1:", "[127.0.0.1]:"),
        ))
        .unwrap(),
    );
    assert_eq!(collect(&task(&s), &p, &store()).await.status, Status::HostKeyFail as i32);
    p.hosts = Arc::new(KnownHosts::parse(&other.known_hosts).unwrap());
    assert_eq!(collect(&task(&s), &p, &store()).await.status, Status::HostKeyFail as i32);
    assert_eq!(s.stats.authentications.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn windows_1251_keeps_raw_bytes() {
    let s = server().await;
    let mut t = task(&s);
    t.commands = vec!["show legacy".into()];
    t.encoding = Some("windows-1251".into());
    let r = collect(&t, &policy(&s), &store()).await;
    assert_eq!(r.status, Status::Ok as i32);
    assert!(r.outputs[0].raw_output.starts_with(&[0xcf, 0xf0]));
    assert!(r.outputs[0].normalized_output.as_ref().unwrap().contains("Привет"));
}
#[tokio::test]
async fn command_timeout_keeps_partial_output_and_closes_socket() {
    let s = server().await;
    let mut t = task(&s);
    t.commands = vec!["show partial".into()];
    t.timeout_ms = 100;
    let r = collect(&t, &policy(&s), &store()).await;
    assert_eq!(r.status, Status::Timeout as i32);
    assert_eq!(r.outputs[0].raw_output, b"partial output without prompt");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(s.stats.active.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn whole_task_timeout_bounds_sequential_commands_and_closes_socket() {
    let s = server().await;
    let mut t = task(&s);
    t.commands = vec!["show slow".into(); 2];
    let mut p = policy(&s);
    p.task_timeout = Duration::from_millis(120);
    assert_eq!(collect(&t, &p, &store()).await.status, Status::Timeout as i32);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(s.stats.active.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn expired_and_invalid_tasks_never_open_ssh() {
    let s = server().await;
    let p = policy(&s);
    let mut expired = task(&s);
    expired.deadline.as_mut().unwrap().seconds -= 100;
    assert_eq!(collect(&expired, &p, &store()).await.status, Status::Expired as i32);
    let mut bads = vec![];
    for command in [
        "",
        "reload",
        "show version; reload",
        "show version\nreload",
        "show x|delete",
        "show $(id)",
        " get version",
        "show \0x",
    ] {
        let mut t = task(&s);
        t.commands = vec![command.into()];
        bads.push(t);
    }
    let mut t = task(&s);
    t.commands = vec!["show version".into(); 33];
    bads.push(t);
    let mut t = task(&s);
    t.commands = vec!["x".repeat(513)];
    bads.push(t);
    let mut t = task(&s);
    t.target = "localhost:22".into();
    bads.push(t);
    let mut t = task(&s);
    t.task_id.clear();
    bads.push(t);
    let mut t = task(&s);
    t.device_id.clear();
    bads.push(t);
    let mut t = task(&s);
    t.credential_ref.clear();
    bads.push(t);
    let mut t = task(&s);
    t.deadline = None;
    bads.push(t);
    let mut t = task(&s);
    t.deadline.as_mut().unwrap().nanos = -1;
    bads.push(t);
    let mut t = task(&s);
    t.encoding = Some("unknown".into());
    bads.push(t);
    for pattern in ["[", "a{1000000000}", "", "a*"] {
        let mut t = task(&s);
        t.prompt_regex = pattern.into();
        if pattern.is_empty() {
            continue;
        }
        bads.push(t);
    }
    let mut t = task(&s);
    t.timeout_ms = u32::MAX;
    bads.push(t);
    for t in bads {
        assert_eq!(collect(&t, &p, &store()).await.status, Status::BadTask as i32, "{t:?}");
    }
    assert_eq!(s.stats.connections.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn oversized_output_and_total_budget_truncate() {
    let s = server().await;
    let mut t = task(&s);
    t.commands = vec!["show large".into()];
    let p = policy(&s);
    let r = collect(&t, &p, &store()).await;
    assert_eq!(r.status, Status::CommandError as i32);
    assert!(r.truncated);
    assert_eq!(r.outputs[0].raw_output.len(), p.max_command_bytes);
    let mut p = p;
    p.max_total_bytes = 45;
    t.commands = vec!["show interfaces".into(); 2];
    let r = collect(&t, &p, &store()).await;
    assert!(r.truncated);
    assert!(r.outputs.iter().map(|o| o.raw_output.len()).sum::<usize>() <= 45);
}
#[tokio::test]
async fn credential_failure_is_sanitized_and_opens_no_socket() {
    let s = server().await;
    let r = collect(&task(&s), &policy(&s), &Store { password: PASSWORD, fail: true }).await;
    assert_eq!(r.status, Status::CredError as i32);
    assert!(!format!("{r:?}").contains(PASSWORD));
    assert_eq!(s.stats.connections.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn closed_channel_and_connection_refused_are_results() {
    let s = server().await;
    let mut t = task(&s);
    t.commands = vec!["show closed".into()];
    assert_eq!(collect(&t, &policy(&s), &store()).await.status, Status::CommandError as i32);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    t.target = listener.local_addr().unwrap().to_string();
    drop(listener);
    assert_eq!(collect(&t, &policy(&s), &store()).await.status, Status::ConnectionError as i32);
}
#[test]
fn known_hosts_is_strict_and_bounded() {
    for input in [
        "",
        "* ssh-ed25519 garbage",
        "|1|hash ssh-ed25519 garbage",
        "@cert-authority x y",
        "localhost ssh-ed25519 garbage",
    ] {
        assert!(KnownHosts::parse(input).is_err());
    }
    assert!(KnownHosts::parse(&" ".repeat(1024 * 1024 + 1)).is_err());
}

#[tokio::test]
async fn connect_handshake_and_auth_timers_are_independent() {
    let s = server_with_auth_delay(Duration::from_millis(300)).await;
    let mut p = policy(&s);
    p.auth_timeout = Duration::from_millis(50);
    let r = collect(&task(&s), &p, &store()).await;
    assert_eq!(r.status, Status::Timeout as i32);
    assert_eq!(s.stats.shells.load(Ordering::SeqCst), 0);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut t = task(&s);
    t.target = listener.local_addr().unwrap().to_string();
    let silent = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    });
    p.connect_timeout = Duration::from_millis(50);
    assert_eq!(collect(&t, &p, &store()).await.status, Status::Timeout as i32);
    silent.abort();
}

#[tokio::test]
async fn initial_prompt_is_required_and_greeting_budget_is_enforced() {
    let s = server().await;
    let mut t = task(&s);
    t.prompt_regex = "never-a-prompt$".into();
    t.timeout_ms = 50;
    let r = collect(&t, &policy(&s), &store()).await;
    assert_eq!(r.status, Status::Timeout as i32);
    assert!(r.outputs.is_empty());
    assert_eq!(s.stats.commands.load(Ordering::SeqCst), 0);
    let mut p = policy(&s);
    p.max_command_bytes = 2;
    let r = collect(&task(&s), &p, &store()).await;
    assert_eq!(r.status, Status::CommandError as i32);
    assert!(r.truncated);
    assert!(r.outputs.is_empty());
}

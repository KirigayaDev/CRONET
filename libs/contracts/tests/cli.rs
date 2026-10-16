use contracts::{
    netmon::v1::{raw_cli_result::Status, CliTask, CommandOutput, RawCliResult},
    subjects,
};
use prost::Message;

#[test]
fn cli_contract_round_trip_and_subjects() {
    let task = CliTask {
        task_id: "task".into(),
        device_id: "device".into(),
        target: "[::1]:22".into(),
        credential_ref: "vault:device".into(),
        commands: vec!["show version".into()],
        prompt_regex: "[#>]\\s*$".into(),
        timeout_ms: 1000,
        deadline: Some(prost_types::Timestamp { seconds: 2_000_000_000, nanos: 0 }),
        encoding: Some("windows-1251".into()),
        attempt: 2,
    };
    assert_eq!(CliTask::decode(task.encode_to_vec().as_slice()).ok(), Some(task));
    let result = RawCliResult {
        status: Status::HostKeyFail.into(),
        outputs: vec![CommandOutput {
            raw_output: vec![0, 255, 27],
            normalized_output: Some("text".into()),
            truncated: true,
            ..Default::default()
        }],
        attempt: 2,
        truncated: true,
        ..Default::default()
    };
    assert_eq!(RawCliResult::decode(result.encode_to_vec().as_slice()).ok(), Some(result));
    assert_eq!(subjects::cli_task("lab"), "tasks.cli.lab");
    assert_eq!(subjects::cli_result("lab"), "raw.cli.lab");
}

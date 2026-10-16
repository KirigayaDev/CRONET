use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // protoc берётся из protoc-bin-vendored: ничего не нужно ставить в систему
    // (важно для Windows и CI).
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../contracts/proto");
    let netmon = root.join("netmon/v1");
    let third_party = root.join("third_party");
    let gnmi = third_party.join("github.com/openconfig/gnmi/proto/gnmi/gnmi.proto");

    let protos = [
        netmon.join("common.proto"),
        netmon.join("poll.proto"),
        netmon.join("trap.proto"),
        netmon.join("gnmi_task.proto"),
        netmon.join("metric.proto"),
        netmon.join("log.proto"),
        gnmi,
    ];

    let include = protoc_bin_vendored::include_path()?;
    tonic_prost_build::configure()
        .build_server(true) // сервер нужен для моков gNMI в тестах
        .build_client(true)
        .compile_protos(&protos, &[root, third_party, include])?;

    println!("cargo:rerun-if-changed=../../contracts/proto");
    Ok(())
}

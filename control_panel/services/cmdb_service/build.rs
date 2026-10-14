fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/cmdb.proto");
    tonic_prost_build::configure().compile_protos(&["proto/cmdb.proto"], &["proto"])?;
    Ok(())
}

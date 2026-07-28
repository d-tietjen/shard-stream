fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/shardstream/v1/stream.proto");
    let mut prost = prost_build::Config::new();
    prost.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure().compile_with_config(
        prost,
        &["proto/shardstream/v1/stream.proto"],
        &["proto"],
    )?;
    Ok(())
}

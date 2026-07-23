fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto/shardstream/v1/stream.proto");
    tonic_prost_build::configure().compile_protos(
        &["../../proto/shardstream/v1/stream.proto"],
        &["../../proto"],
    )?;
    Ok(())
}

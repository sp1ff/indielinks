fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/indielinks.proto");
    tonic_build::configure().compile_protos_with_config(
        tonic_build::Config::default(),
        &["proto/indielinks.proto"],
        &["proto"],
    )?;
    Ok(())
}

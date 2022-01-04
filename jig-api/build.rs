use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let proto_files = &["proto/instrument.proto", "proto/span.proto"];

    let dirs = &["proto"];
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(proto_files, dirs)?;

    // recompile protobufs only if any of the proto files changes.
    for file in proto_files {
        println!("cargo:rerun-if-changed={}", file);
    }
    Ok(())
}

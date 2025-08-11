use std::fs;

/// Build script used to generate Protobuf bindings.
fn main() {
    fs::create_dir_all("src/proto").expect("unable to create proto directory");

    prost_build::Config::new()
        .out_dir("src/proto")
        .compile_protos(&["proto/job.proto"], &["proto"])
        .expect("protobuf compilation failed");

    // Trigger a rebuild when the schema file changes.
    println!("cargo:rerun-if-changed=proto/job.proto");
}

use std::fs;

fn main() {
    // Generate Rust protobuf code directly to src/proto directory
    fs::create_dir_all("src/proto").expect("Failed to create proto directory");

    prost_build::Config::new()
        .out_dir("src/proto")
        .compile_protos(&["proto/job.proto"], &["proto"])
        .expect("Failed to compile proto");

    // Tell Cargo to rerun if proto files change
    println!("cargo:rerun-if-changed=proto/job.proto");
}

use std::io::Result;
fn main() -> Result<()> {
    // Protobuf compilation disabled - using manually generated code in generated.rs
    // prost_build::compile_protos(&["../protos/messages.proto"], &["../protos"])?;
    println!("cargo:rerun-if-changed=../protos/messages.proto");
    Ok(())
}
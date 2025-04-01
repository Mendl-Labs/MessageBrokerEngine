use hostbuilder::{HostedObject, HostedObjectTrait};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let engine = HostedObject::build().expect("Failed to build engine");
    let _ = engine.run().await;
    Ok(())
}
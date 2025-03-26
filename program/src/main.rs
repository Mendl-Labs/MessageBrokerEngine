use hostbuilder::{HostedObject, HostedObjectTrait};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let engine = HostedObject::build()?;
    engine.run().await?;
    Ok(())
}
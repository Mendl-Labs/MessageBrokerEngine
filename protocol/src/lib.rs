pub mod broker {
    pub mod messages {
        include!(concat!(env!("OUT_DIR"), "/broker.messages.rs"));
    }
}
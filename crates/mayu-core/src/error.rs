#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("envelope: {0}")]
    Envelope(String),
    #[error("broker: {0}")]
    Broker(String),
    #[error("config: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, Error>;

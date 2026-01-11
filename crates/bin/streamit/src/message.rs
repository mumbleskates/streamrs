use bilrost::Message;
use bilrost::Oneof;
use time::OffsetDateTime;

// bilrost scored well in this Rust serialization benchmark
// https://github.com/djkoloski/rust_serialization_benchmark

#[derive(Debug, Eq, Clone, Message, Oneof, PartialEq)]
pub enum StreamMessage {
  None,
  #[bilrost(100)]
  Birth(Birth),
  #[bilrost(101)]
  Marriage(String),
  #[bilrost(102)]
  CloseConsumers(String),
}

impl StreamMessage {
  #[must_use]
  pub fn new_close_server() -> Self {
    Self::CloseConsumers(String::from("Server is closing"))
  }
}

impl From<Birth> for StreamMessage {
  fn from(birth: Birth) -> Self {
    Self::Birth(birth)
  }
}

#[derive(Debug, Eq, PartialEq, Clone, Message)]
pub struct Birth {
  name: String,
  born_at_epoch: i64,
}

impl Birth {
  #[must_use]
  pub fn new(name: String) -> Self {
    Self {
      born_at_epoch: OffsetDateTime::now_utc().unix_timestamp(),
      name,
    }
  }
}

#[cfg(test)]
pub mod tests {
  use super::*;
  use bilrost::{Message, OwnedMessage};
  use tokio::test;
  use Birth;

  #[test]
  pub async fn test_serialize_roundtrip() {
    let birth = Birth::new(String::from("Alice"));
    let msg = StreamMessage::from(birth.clone());

    let encoded = msg.encode_to_bytes();
    let decoded = StreamMessage::decode(encoded.as_ref()).unwrap();
    assert_eq!(decoded, msg);
  }
}

mod app;
mod message;

pub use app::TestApp;
pub use message::{encode_simple_string, encode_string, send_message};

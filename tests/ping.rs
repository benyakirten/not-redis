use common::{encode_string, send_message, TestApp};
use not_redis::encoding::simple_string;

mod common;

#[tokio::test]
async fn ping() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("PING");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("PONG"));
}

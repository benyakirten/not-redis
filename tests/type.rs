use common::{encode_string, send_message, TestApp};
use not_redis::encoding::{bulk_string, simple_string};

mod common;

#[tokio::test]
async fn get_type_string() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("type foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("string"));
}

#[tokio::test]
async fn get_type_stream() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("xadd cool 0-1 foo bar");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("0-1"));

    let message = encode_string("type cool");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("stream"));
}

#[tokio::test]
async fn get_type_missing() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("type foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("none"));
}

use common::{encode_string, send_message, TestApp};
use not_redis::encoding::{bulk_string, empty_string, encode_array, simple_string};

mod common;

#[tokio::test]
async fn test_set_get_string_success() {
    let test_app = TestApp::master().await;

    let message = encode_string("set foo bar");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("get foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, bulk_string("bar"));
}

#[tokio::test]
async fn test_get_missing_item() {
    let test_app = TestApp::master().await;
    let message = encode_string("get foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, empty_string());
}

#[tokio::test]
async fn test_set_missing_value() {
    let test_app = TestApp::master().await;
    let message = encode_string("set foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    // TODO: Fix this when we have proper error handling.
    assert_eq!(resp, "");
}

#[tokio::test]
async fn test_set_missing_key_value() {
    let test_app = TestApp::master().await;
    let message = encode_string("set");
    let resp = send_message(&test_app.address.name(), &message).await;
    // TODO: Fix this when we have proper error handling.
    assert_eq!(resp, "");
}

#[tokio::test]
async fn test_get_database_keys() {
    let test_app = TestApp::master().await;

    let message = encode_string("keys *");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, encode_array(&[]));

    let message = encode_string("set foo bar");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("set baz bat");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("keys *");
    let resp = send_message(&test_app.address.name(), &message).await;

    assert!(resp.contains(&bulk_string("foo")));
    assert!(resp.contains(&bulk_string("baz")));
}

// TODO: Add tests for expiry

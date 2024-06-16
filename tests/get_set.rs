use common::{encode_string, send_message, TestApp};
use not_redis::encoding::{bulk_string, empty_string, encode_array, simple_string};

mod common;

#[tokio::test]
async fn set_get_string_success() {
    let test_app = TestApp::master().await;

    let message = encode_string("set foo bar");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("get foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, bulk_string("bar"));
}

#[tokio::test]
async fn get_missing_item() {
    let test_app = TestApp::master().await;
    let message = encode_string("get foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, empty_string());
}

#[tokio::test]
async fn set_missing_value() {
    let test_app = TestApp::master().await;
    let message = encode_string("set foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    // TODO: Fix this when we have proper error handling.
    assert_eq!(resp, "");
}

#[tokio::test]
async fn set_missing_key_value() {
    let test_app = TestApp::master().await;
    let message = encode_string("set");
    let resp = send_message(&test_app.address.name(), &message).await;
    // TODO: Fix this when we have proper error handling.
    assert_eq!(resp, "");
}

#[tokio::test]
async fn get_database_keys() {
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

#[tokio::test]
async fn set_get_string_with_expiry() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar px 100");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("get foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, bulk_string("bar"));

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let message = encode_string("get foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, empty_string());
}

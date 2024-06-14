use common::{
    empty_string, encode_array_string_item, encode_simple_string, encode_string, send_message,
};

mod common;

#[tokio::test]
async fn test_set_get_string_success() {
    let test_app = common::TestApp::simple().await;

    let message = encode_string("set foo bar");
    let resp = send_message(&test_app.address.name(), &message, 512).await;
    assert_eq!(resp, encode_simple_string("OK"));

    let message = encode_string("get foo");
    let resp = send_message(&test_app.address.name(), &message, 512).await;
    assert_eq!(resp, encode_array_string_item("bar"));
}

#[tokio::test]
async fn test_get_missing_item() {
    let test_app = common::TestApp::simple().await;
    let message = encode_string("get foo");
    let resp = send_message(&test_app.address.name(), &message, 512).await;
    assert_eq!(resp, empty_string());
}

#[tokio::test]
async fn test_set_missing_value() {
    let test_app = common::TestApp::simple().await;
    let message = encode_string("set foo");
    let resp = send_message(&test_app.address.name(), &message, 512).await;
    // TODO: Fix this when we have proper error handling.
    assert_eq!(resp, "");
}

#[tokio::test]
async fn test_set_missing_key_value() {
    let test_app = common::TestApp::simple().await;
    let message = encode_string("set");
    let resp = send_message(&test_app.address.name(), &message, 512).await;
    // TODO: Fix this when we have proper error handling.
    assert_eq!(resp, "");
}

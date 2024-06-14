use common::{encode_array_string_item, encode_string, send_message};

mod common;

#[tokio::test]
async fn test_echo_success() {
    let test_app = common::TestApp::simple().await;

    let message = encode_string("echo hello");
    let resp = send_message(&test_app.address.name(), &message, 512).await;
    assert_eq!(resp, encode_array_string_item("hello"));
}

#[tokio::test]
async fn test_echo_fail_if_short() {
    let test_app = common::TestApp::simple().await;

    let message = encode_string("echo");
    let resp = send_message(&test_app.address.name(), &message, 512).await;
    // TODO: Fix this when we have proper error handling.
    assert_eq!(resp, "");
}

#[tokio::test]
async fn test_echo_fail_if_long() {
    let test_app = common::TestApp::simple().await;

    let message = encode_string("echo hello bye");
    let resp = send_message(&test_app.address.name(), &message, 512).await;
    // TODO: Fix this when we have proper error handling.
    assert_eq!(resp, "");
}

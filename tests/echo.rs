use common::{encode_array_string_item, encode_string, send_message, TestApp};

mod common;

#[tokio::test]
async fn test_echo_success() {
    let test_app = TestApp::master().await;

    let message = encode_string("echo hello");
    let resp = send_message(&test_app.address.name(), &message, ).await;
    assert_eq!(resp, encode_array_string_item("hello"));
}

#[tokio::test]
async fn test_echo_fail_if_short() {
    let test_app = TestApp::master().await;

    let message = encode_string("echo");
    let resp = send_message(&test_app.address.name(), &message, ).await;
    // TODO: Fix this when we have proper error handling.
    assert_eq!(resp, "");
}

#[tokio::test]
async fn test_echo_fail_if_long() {
    let test_app = TestApp::master().await;

    let message = encode_string("echo hello bye");
    let resp = send_message(&test_app.address.name(), &message, ).await;
    // TODO: Fix this when we have proper error handling.
    assert_eq!(resp, "");
}

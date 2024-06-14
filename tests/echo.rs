use common::{encode_simple_string, encode_string, send_message};

mod common;

#[tokio::test]
async fn test_echo_success() {
    let test_app = common::TestApp::simple().await;

    let message = encode_string("echo hello");
    let (resp, read_len) = send_message(&test_app.address.name(), &message, 512).await;
    let resp = String::from_utf8(resp[..read_len].to_vec()).unwrap();
    assert_eq!(resp, encode_simple_string("hello"));
}

#[tokio::test]
async fn test_echo_fail_if_short() {
    let test_app = common::TestApp::simple().await;

    let message = encode_string("echo");
    let (resp, read_len) = send_message(&test_app.address.name(), &message, 512).await;
    let resp = String::from_utf8(resp[..read_len].to_vec()).unwrap();
    // TODO: Fix this when we have proper error handling.
    assert_eq!(resp, "");
}

#[tokio::test]
async fn test_echo_fail_if_long() {
    let test_app = common::TestApp::simple().await;

    let message = encode_string("echo hello bye");
    let (resp, read_len) = send_message(&test_app.address.name(), &message, 512).await;
    let resp = String::from_utf8(resp[..read_len].to_vec()).unwrap();
    // TODO: Fix this when we have proper error handling.
    assert_eq!(resp, "");
}

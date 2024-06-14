use common::{encode_array_string_item, encode_simple_string, encode_string, send_message};

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

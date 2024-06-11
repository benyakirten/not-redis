use common::{encode_string, send_message};

mod common;

#[tokio::test]
async fn test_ping() {
    let test_app = common::TestApp::simple().await;
    let address = test_app.address.name();

    let message = encode_string("PING");
    let (resp, read_len) = send_message(&address, &message, 512).await;
    let resp = String::from_utf8(resp[..read_len].to_vec()).unwrap();
    assert_eq!(resp, "+PONG\r\n");
}

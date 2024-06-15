use common::{encode_string, send_message, TestApp};

mod common;

#[tokio::test]
async fn test_ping() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("PING");
    let resp = send_message(&address, &message, 512).await;
    assert_eq!(resp, "+PONG\r\n");
}

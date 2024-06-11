use common::send_message;

mod common;

#[tokio::test]
async fn test_ping() {
    let test_app = common::setup().await;
    let address = test_app.address.name();

    let (resp, _) = send_message(&address, "PING\r\n", 512).await;
    let resp = String::from_utf8(resp).unwrap();
    assert_eq!(resp, "+PONG\r\n");
}

use common::send_message;

mod common;

#[tokio::test]
async fn test_ping() {
    let test_app = common::setup().await;
    let address = test_app.address.name();

    let (resp, read_len) = send_message(&address, "*1\r\n$4\r\nPING\r\n", 512).await;
    let resp = String::from_utf8(resp[..read_len].to_vec()).unwrap();
    assert_eq!(resp, "+PONG\r\n");
}

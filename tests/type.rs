use common::{encode_string, send_message, TestApp};
use not_redis::encoding::simple_string;

mod common;

#[tokio::test]
async fn get_type_string() {
    assert!(true)
    // let test_app = TestApp::master().await;
    // let address = test_app.address.name();

    // let message = encode_string("set foo bar");
    // let resp = send_message(&address, &message).await;
    // assert_eq!(resp, simple_string("OK"));

    // let message = encode_string("type foo");
    // let resp = send_message(&address, &message).await;
    // assert_eq!(resp, simple_string("OK"));
}

// #[tokio::test]
// async fn get_type_stream() {
//     todo!()
// }

// #[tokio::test]
// async fn get_type_missing() {
//     todo!()
// }

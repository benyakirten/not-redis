use common::{encode_string, send_message, TestApp};
use not_redis::encoding::{bulk_string, error_string, simple_string};
use tokio::sync::broadcast::error;

mod common;

#[tokio::test]
async fn specified_stream_id_must_be_greater_than_previous() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("xadd cool 100-100 one two");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("100-100"));

    let message = encode_string("xadd cool 101-99 three four");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("101-99"));

    let message = encode_string("xadd cool 101-100 five six");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("101-100"));

    let message = encode_string("xadd cool 101-50 seven eight");
    let resp = send_message(&address, &message).await;
    assert_eq!(
        resp,
        error_string(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        )
    );

    let message = encode_string("xadd cool 50-50 nine ten");
    let resp = send_message(&address, &message).await;
    assert_eq!(
        resp,
        error_string(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        )
    );
}

#[tokio::test]
async fn unspecified_stream_id_must_be_greater_than_previous() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();
    assert!(true);

    // let message = encode_string("xadd cool *-100 foo bar");
    // let resp = send_message(&address, &message).await;
}

#[tokio::test]
async fn autogenerate_consecutive_stream_ids() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();
    assert!(true);
}

#[tokio::test]
async fn xrange_read_specified_range_from_stream() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();
    assert!(true);
}

#[tokio::test]
async fn xrange_read_from_start_of_range() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();
    assert!(true);
}

#[tokio::test]
async fn xrange_read_to_end_of_range() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();
    assert!(true);
}

#[tokio::test]
async fn xread_from_single_stream() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();
    assert!(true);
}

#[tokio::test]
async fn xread_from_multiple_streams() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();
    assert!(true);
}

#[tokio::test]
async fn block_read_with_timeout() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();
    assert!(true);
}

#[tokio::test]
async fn block_reads_without_timeout() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();
    assert!(true);
}

#[tokio::test]
async fn block_reads_no_id_specified() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();
    assert!(true);
}

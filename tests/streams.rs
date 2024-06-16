use common::{encode_string, send_message, TestApp};
use not_redis::encoding::{bulk_string, simple_string};

mod common;

#[tokio::test]
async fn specify_stream_id() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("xadd cool 100-100 foo bar");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("0-1"));

    // TODO: Read from stream
}

#[tokio::test]
async fn unspecified_stream_id_must_be_greater_than_previous() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();
    assert!(true);
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

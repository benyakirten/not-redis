use common::{encode_stream_items, encode_string, send_message, StreamData, TestApp};
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
async fn unspecified_stream_id_be_greater_than_previous() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();
    assert!(true);

    let message = encode_string("xadd cool 100-* one two");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("100-0"));

    let message = encode_string("xadd cool 100-* one two");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("100-1"));
}

#[tokio::test]
async fn autogenerate_consecutive_stream_ids() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("xadd cool * one two");
    let resp = send_message(&address, &message).await;

    let got_msg: Vec<&str> = resp.split_whitespace().skip(1).collect();
    let got_pieces = got_msg[0].split('-').collect::<Vec<&str>>();

    let got_id_1 = got_pieces[0].parse::<i64>().unwrap();

    let message = encode_string("xadd cool * three four");
    let resp = send_message(&address, &message).await;

    let got_msg: Vec<&str> = resp.split_whitespace().skip(1).collect();
    let got_pieces = got_msg[0].split('-').collect::<Vec<&str>>();

    let got_id_2 = got_pieces[0].parse::<i64>().unwrap();
    assert!(got_id_2 > got_id_1);
}

#[tokio::test]
async fn xrange_read_specified_range_from_stream() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("xadd cool 100-50 one two three four");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 100-100 five six");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 101-99 seven eight nine ten");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 101-100 eleven twelve");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 102-99 thirteen fourteen fifteen sixteen");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 102-100 seventeen eighteen");
    send_message(&address, &message).await;

    let message = encode_string("xrange cool 100 102");
    let resp = send_message(&address, &message).await;

    let stream_items: Vec<StreamData<'_>> = vec![
        StreamData {
            id: "100-50",
            items: vec!["one", "two", "three", "four"],
        },
        StreamData {
            id: "100-100",
            items: vec!["five", "six"],
        },
        StreamData {
            id: "101-99",
            items: vec!["seven", "eight", "nine", "ten"],
        },
        StreamData {
            id: "101-100",
            items: vec!["eleven", "twelve"],
        },
    ];
    let want_streams = encode_stream_items(stream_items);

    assert_eq!(resp, want_streams);
}

#[tokio::test]
async fn xrange_read_from_start_of_range() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("xadd cool 100-50 one two three four");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 100-100 five six");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 101-99 seven eight nine ten");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 101-100 eleven twelve");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 102-99 thirteen fourteen fifteen sixteen");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 102-100 seventeen eighteen");
    send_message(&address, &message).await;

    let message = encode_string("xrange cool - 102");
    let resp = send_message(&address, &message).await;

    let stream_items: Vec<StreamData<'_>> = vec![
        StreamData {
            id: "100-50",
            items: vec!["one", "two", "three", "four"],
        },
        StreamData {
            id: "100-100",
            items: vec!["five", "six"],
        },
        StreamData {
            id: "101-99",
            items: vec!["seven", "eight", "nine", "ten"],
        },
        StreamData {
            id: "101-100",
            items: vec!["eleven", "twelve"],
        },
    ];
    let want_streams = encode_stream_items(stream_items);

    assert_eq!(resp, want_streams);
}

#[tokio::test]
async fn xrange_read_to_end_of_range() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("xadd cool 100-50 one two three four");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 100-100 five six");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 101-99 seven eight nine ten");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 101-100 eleven twelve");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 102-99 thirteen fourteen fifteen sixteen");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 102-100 seventeen eighteen");
    send_message(&address, &message).await;

    let message = encode_string("xrange cool 101 +");
    let resp = send_message(&address, &message).await;

    let stream_items: Vec<StreamData<'_>> = vec![
        StreamData {
            id: "101-99",
            items: vec!["seven", "eight", "nine", "ten"],
        },
        StreamData {
            id: "101-100",
            items: vec!["eleven", "twelve"],
        },
        StreamData {
            id: "102-99",
            items: vec!["thirteen", "fourteen", "fifteen", "sixteen"],
        },
        StreamData {
            id: "102-100",
            items: vec!["seventeen", "eighteen"],
        },
    ];
    let want_streams = encode_stream_items(stream_items);

    assert_eq!(resp, want_streams);
}

#[tokio::test]
async fn xrange_full_range() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("xadd cool 100-50 one two three four");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 100-100 five six");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 101-99 seven eight nine ten");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 101-100 eleven twelve");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 102-99 thirteen fourteen fifteen sixteen");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 102-100 seventeen eighteen");
    send_message(&address, &message).await;

    let message = encode_string("xrange cool - +");
    let resp = send_message(&address, &message).await;

    let stream_items: Vec<StreamData<'_>> = vec![
        StreamData {
            id: "100-50",
            items: vec!["one", "two", "three", "four"],
        },
        StreamData {
            id: "100-100",
            items: vec!["five", "six"],
        },
        StreamData {
            id: "101-99",
            items: vec!["seven", "eight", "nine", "ten"],
        },
        StreamData {
            id: "101-100",
            items: vec!["eleven", "twelve"],
        },
        StreamData {
            id: "102-99",
            items: vec!["thirteen", "fourteen", "fifteen", "sixteen"],
        },
        StreamData {
            id: "102-100",
            items: vec!["seventeen", "eighteen"],
        },
    ];
    let want_streams = encode_stream_items(stream_items);

    assert_eq!(resp, want_streams);
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

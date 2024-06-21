use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use common::{
    encode_stream_items, encode_streams, encode_string, send_message, StreamData, StreamItem,
    TestApp,
};
use not_redis::encoding::{bulk_string, empty_string, error_string};

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

    sleep(Duration::from_millis(100)).await;

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

    let stream_items: Vec<StreamItem<'_>> = vec![
        StreamItem {
            id: "100-50",
            items: vec!["one", "two", "three", "four"],
        },
        StreamItem {
            id: "100-100",
            items: vec!["five", "six"],
        },
        StreamItem {
            id: "101-99",
            items: vec!["seven", "eight", "nine", "ten"],
        },
        StreamItem {
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

    let stream_items: Vec<StreamItem<'_>> = vec![
        StreamItem {
            id: "100-50",
            items: vec!["one", "two", "three", "four"],
        },
        StreamItem {
            id: "100-100",
            items: vec!["five", "six"],
        },
        StreamItem {
            id: "101-99",
            items: vec!["seven", "eight", "nine", "ten"],
        },
        StreamItem {
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

    let stream_items: Vec<StreamItem<'_>> = vec![
        StreamItem {
            id: "101-99",
            items: vec!["seven", "eight", "nine", "ten"],
        },
        StreamItem {
            id: "101-100",
            items: vec!["eleven", "twelve"],
        },
        StreamItem {
            id: "102-99",
            items: vec!["thirteen", "fourteen", "fifteen", "sixteen"],
        },
        StreamItem {
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

    let stream_items: Vec<StreamItem<'_>> = vec![
        StreamItem {
            id: "100-50",
            items: vec!["one", "two", "three", "four"],
        },
        StreamItem {
            id: "100-100",
            items: vec!["five", "six"],
        },
        StreamItem {
            id: "101-99",
            items: vec!["seven", "eight", "nine", "ten"],
        },
        StreamItem {
            id: "101-100",
            items: vec!["eleven", "twelve"],
        },
        StreamItem {
            id: "102-99",
            items: vec!["thirteen", "fourteen", "fifteen", "sixteen"],
        },
        StreamItem {
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

    let message = encode_string("xadd cool 100-50 one two three four");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 100-100 five six");
    send_message(&address, &message).await;

    let message = encode_string("xread streams cool 100-75");
    let resp = send_message(&address, &message).await;

    let stream_items: Vec<StreamItem<'_>> = vec![StreamItem {
        id: "100-100",
        items: vec!["five", "six"],
    }];

    let stream_data = StreamData {
        name: "cool",
        items: stream_items,
    };

    let want = encode_streams(vec![stream_data]);

    assert_eq!(resp, want);
}

#[tokio::test]
async fn xread_from_multiple_streams() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("xadd cool 100-50 one two three four");
    send_message(&address, &message).await;

    let message = encode_string("xadd cooler 1000-1000 five six");
    send_message(&address, &message).await;

    let message = encode_string("xread streams cool cooler 100-0 1000-500");
    let resp = send_message(&address, &message).await;

    let stream_1_items: Vec<StreamItem<'_>> = vec![StreamItem {
        id: "100-50",
        items: vec!["one", "two", "three", "four"],
    }];

    let stream_1_data = StreamData {
        name: "cool",
        items: stream_1_items,
    };

    let stream_2_items: Vec<StreamItem<'_>> = vec![StreamItem {
        id: "1000-1000",
        items: vec!["five", "six"],
    }];

    let stream_2_data = StreamData {
        name: "cooler",
        items: stream_2_items,
    };

    let want = encode_streams(vec![stream_1_data, stream_2_data]);
    assert_eq!(resp, want);
}

#[tokio::test]
async fn block_timeout_no_new_range_items() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("xadd cool 100-100 five six");
    send_message(&address, &message).await;

    let join_handle = tokio::spawn(async move {
        let message = encode_string("xread block 100 streams cool 100-50");
        send_message(&address, &message).await
    });

    let block_resp = join_handle.await.unwrap();

    assert_eq!(block_resp, empty_string());
}

#[tokio::test]
async fn block_timeout_add_new_range_items() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let addr = address.clone();

    let join_handle = tokio::spawn(async move {
        let message = encode_string("xread block 500 streams cool 0");
        send_message(&addr, &message).await
    });

    // The block command has to be sent first. I'm struggling of a good way to ensure that
    // the block command is activated since Rust has to muster the thread. A 100ms timeout
    // is hopefully enough.
    sleep(Duration::from_millis(100)).await;

    let message = encode_string("xadd cool 100-100 five six");
    send_message(&address, &message).await;

    let block_resp = join_handle.await.unwrap();

    let stream_items: Vec<StreamItem<'_>> = vec![StreamItem {
        id: "100-100",
        items: vec!["five", "six"],
    }];

    let stream_data = StreamData {
        name: "cool",
        items: stream_items,
    };
    let want_streams = encode_streams(vec![stream_data]);

    assert_eq!(block_resp, want_streams);
}

#[tokio::test]
async fn block_reads_without_timeout_resolves_on_new_entries_greater_than_id() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let addr = address.clone();

    let is_resolved = Arc::new(Mutex::new(false));

    let item_resolved = is_resolved.clone();
    let join_handle = tokio::spawn(async move {
        let message = encode_string("xread block 0 streams cool 100");
        let resp = send_message(&addr, &message).await;
        {
            *item_resolved.lock().await = true;
        }

        resp
    });

    sleep(Duration::from_millis(500)).await;
    {
        let has_been_resolved = is_resolved.lock().await;
        assert!(!*has_been_resolved);
    }

    let message = encode_string("xadd cool 50-100 three four");
    send_message(&address, &message).await;

    sleep(Duration::from_millis(100)).await;
    {
        let has_been_resolved = is_resolved.lock().await;
        assert!(!*has_been_resolved);
    }

    let message = encode_string("xadd cool 150-100 five six");
    send_message(&address, &message).await;

    sleep(Duration::from_millis(100)).await;
    {
        let has_been_resolved = is_resolved.lock().await;
        assert!(*has_been_resolved);
    }

    let block_resp = join_handle.await.unwrap();

    let stream_items: Vec<StreamItem<'_>> = vec![StreamItem {
        id: "150-100",
        items: vec!["five", "six"],
    }];

    let stream_data = StreamData {
        name: "cool",
        items: stream_items,
    };
    let want_streams = encode_streams(vec![stream_data]);

    assert_eq!(block_resp, want_streams);
}

#[tokio::test]
async fn block_reads_with_no_id_specified_returns_all_new_entries() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let addr = address.clone();

    let join_handle = tokio::spawn(async move {
        let message = encode_string("xread block 200 streams cool $");
        send_message(&addr, &message).await
    });

    sleep(Duration::from_millis(100)).await;

    let message = encode_string("xadd cool 1-0 five six");
    send_message(&address, &message).await;

    let message = encode_string("xadd cool 2-0 seven eight");
    send_message(&address, &message).await;

    let block_resp = join_handle.await.unwrap();

    let stream_items: Vec<StreamItem<'_>> = vec![
        StreamItem {
            id: "1-0",
            items: vec!["five", "six"],
        },
        StreamItem {
            id: "2-0",
            items: vec!["seven", "eight"],
        },
    ];

    let stream_data = StreamData {
        name: "cool",
        items: stream_items,
    };
    let want_streams = encode_streams(vec![stream_data]);

    assert_eq!(block_resp, want_streams);
}

#[tokio::test]
async fn receive_errors_if_item_not_stream() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar");
    send_message(&address, &message).await;

    let message = encode_string("xadd foo 100-50 one two three four");
    let response = send_message(&address, &message).await;
    assert_eq!(
        response,
        error_string("WRONGTYPE Operation against a key holding the wrong kind of value")
    );

    let message = encode_string("xrange foo 100 102");
    let response = send_message(&address, &message).await;
    assert_eq!(
        response,
        error_string("WRONGTYPE Operation against a key holding the wrong kind of value")
    );

    let message = encode_string("xread streams cool 100-75");
    let response = send_message(&address, &message).await;
    assert_eq!(
        response,
        error_string("WRONGTYPE Operation against a key holding the wrong kind of value")
    );

    let message = encode_string("get foo");
    let response = send_message(&address, &message).await;
    assert_eq!(response, bulk_string("bar"));
}

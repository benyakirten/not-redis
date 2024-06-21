use tokio::time::{sleep, Duration};

use common::{encode_string, send_message, TestApp};
use not_redis::encoding::{
    bulk_string, empty_string, encode_integer, encode_string_array, error_string, simple_string,
};

mod common;

#[tokio::test]
async fn set_get_string_success() {
    let test_app = TestApp::master().await;

    let message = encode_string("set foo bar");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("get foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, bulk_string("bar"));
}

#[tokio::test]
async fn get_missing_item() {
    let test_app = TestApp::master().await;
    let message = encode_string("get foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, empty_string());
}

#[tokio::test]
async fn set_missing_value() {
    let test_app = TestApp::master().await;
    let message = encode_string("set foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, "Missing value: SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |\n  EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]");
}

#[tokio::test]
async fn set_missing_key_value() {
    let test_app = TestApp::master().await;
    let message = encode_string("set");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, "Missing key: SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |\n  EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]");
}

#[tokio::test]
async fn get_database_keys() {
    let test_app = TestApp::master().await;

    let message = encode_string("keys *");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, encode_string_array(&[]));

    let message = encode_string("set foo bar");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("set baz bat");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("keys *");
    let resp = send_message(&test_app.address.name(), &message).await;

    assert!(resp.contains(&bulk_string("foo")));
    assert!(resp.contains(&bulk_string("baz")));
}

#[tokio::test]
async fn set_get_string_with_expiry() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar px 100");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("get foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, bulk_string("bar"));

    sleep(Duration::from_millis(100)).await;

    let message = encode_string("get foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, empty_string());
}

#[tokio::test]
async fn getdel_gets_then_deletes_key() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar px 500");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("getdel foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("bar"));

    let message = encode_string("get foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, empty_string());

    let message = encode_string("set foo bar");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    sleep(Duration::from_millis(500)).await;

    let message = encode_string("get foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("bar"));
}

#[tokio::test]
async fn set_overwrites_expiration_time() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar px 200");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("set foo baz");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    sleep(Duration::from_millis(300)).await;

    let message = encode_string("get foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("baz"));
}

#[tokio::test]
async fn set_keepttl_does_not_overwrite_expiration_time() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar px 500");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("set foo baz keepttl");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    sleep(Duration::from_millis(500)).await;

    let message = encode_string("get foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, empty_string());
}

#[tokio::test]
async fn set_overwrite_options_behave_as_expected() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("set foo baz xx");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("get foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("baz"));

    let message = encode_string("set bar baz xx");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("get bar");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, empty_string());

    let message = encode_string("set foo bat nx");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("get foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("baz"));
}

#[tokio::test]
async fn set_no_overwrite_value_keeps_value_changes_expiration() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar px 300");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("set foo baz nx");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    sleep(Duration::from_millis(500)).await;

    let message = encode_string("get foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("bar"));
}

#[tokio::test]
async fn set_get_previous_value() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("set foo baz get");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("bar"));
}

#[tokio::test]
async fn del_removes_item_stops_expiration() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar px 300");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("del foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, encode_integer(1));

    let message = encode_string("get foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, empty_string());

    let message = encode_string("set foo bar");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    sleep(Duration::from_millis(500)).await;

    let message = encode_string("get foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("bar"));
}

#[tokio::test]
async fn del_removes_multiple_items() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("set baz bat");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("del foo baz");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, encode_integer(2));

    let message = encode_string("get foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, empty_string());

    let message = encode_string("get baz");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, empty_string());
}

#[tokio::test]
async fn getex_changes_item_expiration() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar px 300");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("getex foo persist");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("bar"));

    sleep(Duration::from_millis(500)).await;

    let message = encode_string("get foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("bar"));
}

#[tokio::test]
async fn incr_decr_num_string() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo 1");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("incr foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, encode_integer(2));

    let message = encode_string("decr foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, encode_integer(1));

    let message = encode_string("decrby foo 2");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, encode_integer(-1));

    let message = encode_string("incrby foo -2");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, encode_integer(-3));

    let message = encode_string("incrby foo 2");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, encode_integer(-1));

    let message = encode_string("incrbyfloat foo 3.1");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("2.1"));
}

#[tokio::test]
async fn incr_decr_non_number_string() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("set foo bar");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, simple_string("OK"));

    let message = encode_string("incr foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(
        resp,
        error_string("ERR value is not an integer or out of range")
    );

    let message = encode_string("decr foo");
    let resp = send_message(&address, &message).await;
    assert_eq!(
        resp,
        error_string("ERR value is not an integer or out of range")
    );
}

#[tokio::test]
async fn cannot_set_invalid_expiration_time() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string(&format!("set cool cooler ex {}", 10e100));
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, "ERR value is not an integer or out of range");

    let message = encode_string(&format!("getex cool px {}", 10e100));
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, "ERR value is not an integer or out of range");

    let message = encode_string(&format!("set cool cooler pxat {}", 10e100));
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, "ERR value is not an integer or out of range");

    let message = encode_string("set cool cooler exat hello");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, "ERR value is not an integer or out of range");
}

#[tokio::test]
async fn cannot_get_item_not_a_string() {
    let test_app = TestApp::master().await;
    let address = test_app.address.name();

    let message = encode_string("xadd cool 100-* one two");
    let resp = send_message(&address, &message).await;
    assert_eq!(resp, bulk_string("100-0"));

    let message = encode_string("get cool");
    let resp = send_message(&address, &message).await;
    assert_eq!(
        resp,
        error_string("WRONGTYPE Operation against a key holding the wrong kind of value")
    );

    let message = encode_string("getex cool px 1000");
    let resp = send_message(&address, &message).await;
    assert_eq!(
        resp,
        error_string("WRONGTYPE Operation against a key holding the wrong kind of value")
    );

    let message = encode_string("set cool cooler get");
    let resp = send_message(&address, &message).await;
    assert_eq!(
        resp,
        error_string("WRONGTYPE Operation against a key holding the wrong kind of value")
    );
}

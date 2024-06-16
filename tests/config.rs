use not_redis::encoding::{bulk_string, empty_string, encode_string_array};
use not_redis::server::Config;

use common::{encode_string, send_message, TestApp};

mod common;

#[tokio::test]
async fn get_empty_config() {
    let test_app = TestApp::master().await;

    let message = encode_string("config get dir");
    let resp = send_message(&test_app.address.name(), &message).await;
    let want_dir = encode_string_array(&vec!["dir", ""]);
    assert_eq!(resp, want_dir);

    let message = encode_string("config get dbfilename");
    let resp = send_message(&test_app.address.name(), &message).await;
    let want_dbfilename = encode_string_array(&vec!["dbfilename", ""]);
    assert_eq!(resp, want_dbfilename);
}

#[tokio::test]
async fn empty_database_if_unable_to_find_rdb() {
    let config = Config::new(Some("./test".into()), Some("database.rdb".into()));
    let test_app = TestApp::with_config(config).await;

    let message = encode_string("config get dir");
    let resp = send_message(&test_app.address.name(), &message).await;
    let want_dir = encode_string_array(&vec!["dir", "./test"]);
    assert_eq!(resp, want_dir);

    let message = encode_string("config get dbfilename");
    let resp = send_message(&test_app.address.name(), &message).await;
    let want_dbfilename = encode_string_array(&vec!["dbfilename", "database.rdb"]);
    assert_eq!(resp, want_dbfilename);

    let message = encode_string("get foo");
    let resp = send_message(&test_app.address.name(), &message).await;
    assert_eq!(resp, empty_string());
}

#[tokio::test]
async fn preset_values_from_valid_rdb() {
    let config = Config::new(Some("tests/test_data".into()), Some("dump_1.rdb".into()));
    let test_app = TestApp::with_config(config).await;

    let message = encode_string("get bat");
    let resp = send_message(&test_app.address.name(), &message).await;

    assert_eq!(resp, bulk_string("baz"));
}

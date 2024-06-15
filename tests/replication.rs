use common::{
    encode_array_string_item, encode_simple_string, encode_string, send_message, TestApp,
};

mod common;

#[tokio::test]
pub async fn test_info_master() {
    let test_app = TestApp::master().await;

    let (repl_id, repl_offset) = {
        let repl = &test_app.redis_server.read().await.replication;
        let repl_id = repl.id.clone();
        let repl_offset = repl.offset.clone();
        (repl_id, repl_offset)
    };

    let message = encode_string("info replication");
    let resp = send_message(&test_app.address.name(), &message).await;

    let want_role = "role:master";
    let want_repl_id = format!("master_replid:{}", repl_id);
    let want_repl_offset = format!("master_repl_offset:{}", repl_offset);

    assert!(resp.contains(want_role));
    assert!(resp.contains(&want_repl_id));
    assert!(resp.contains(&want_repl_offset));
}

#[tokio::test]
pub async fn test_info_slave() {
    let test_app_master = TestApp::master().await;
    let repl_id = {
        let repl = &test_app_master.redis_server.read().await.replication;
        repl.id.clone()
    };
    let test_app_slave = TestApp::slave(test_app_master.address.clone()).await;

    let message = encode_string("info replication");
    let resp = send_message(&test_app_slave.address.name(), &message).await;

    let want_role = "role:slave";
    let want_repl_id = format!("master_replid:{}", repl_id);
    let want_repl_offset = format!("master_repl_offset:{}", 0);

    assert!(resp.contains(want_role));
    assert!(resp.contains(&want_repl_id));
    assert!(resp.contains(&want_repl_offset));
}

#[tokio::test]
pub async fn test_set_replicated_to_slave() {
    let test_app_master = TestApp::master().await;
    let test_app_slave = TestApp::slave(test_app_master.address.clone()).await;

    let message = encode_string("set foo bar");
    let resp = send_message(&test_app_master.address.name(), &message).await;

    assert_eq!(resp, encode_simple_string("OK"));

    let message = encode_string("get foo");
    let resp = send_message(&test_app_slave.address.name(), &message).await;

    assert_eq!(resp, encode_array_string_item("bar"));
}

// TODO: Test wait when it's fixed

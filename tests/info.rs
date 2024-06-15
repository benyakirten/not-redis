use common::{encode_string, send_message, TestApp};

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
    let resp = send_message(&test_app.address.name(), &message, 512).await;

    let want_role = "role:master";
    let want_repl_id = format!("master_replid:{}", repl_id);
    let want_repl_offset = format!("master_repl_offset:{}", repl_offset);

    assert!(resp.contains(want_role));
    assert!(resp.contains(&want_repl_id));
    assert!(resp.contains(&want_repl_offset));
}

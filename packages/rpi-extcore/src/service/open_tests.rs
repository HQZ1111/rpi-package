//! A warm open must not queue behind another session's native start.
use super::*;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};

/// Insert a live host whose native peer answers nothing but `stop`.
async fn live_host(state: &Service, id: &str, project_id: &str) -> tokio::task::JoinHandle<()> {
    let (writer, peer) = tokio::io::duplex(8192);
    let ready = json!({"type":"ready","protocol":"rpi","supportedCommands":["get_state"]});
    let (host, _injector) = HostSession::test_connection_with_ready(writer, 16, ready);
    state
        .registry
        .write()
        .await
        .upsert(RegistryEntry {
            session_id: id.into(),
            project_id: Some(project_id.into()),
            title: None,
            parent_session_id: None,
            fork_entry_id: None,
            relation: None,
            created_at: 0,
            updated_at: 0,
        })
        .unwrap();
    state.sessions.write().await.insert(id.into(), host);
    tokio::spawn(async move {
        let mut lines = BufReader::new(peer).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let request: Value = serde_json::from_str(&line).unwrap();
            if request["type"] == "stop" {
                break;
            }
        }
    })
}

#[tokio::test]
async fn warm_open_does_not_wait_for_another_sessions_native_start() {
    let base = std::fs::canonicalize(std::env::temp_dir()).unwrap();
    let root = base.join(format!("rpi-open-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(root.join("sessions")).unwrap();
    let service = Service::new(BackendConfig {
        project: root.clone(),
        registry: root.join("registry.json"),
        session_dir: Some(root.join("sessions")),
        rpi_bin: root.join("never-spawn.exe"),
        ..BackendConfig::default()
    })
    .unwrap();
    let live = live_host(&service, "warm", &service.default_project_id).await;

    // Stand in for a concurrent restore: the lifecycle lock is held for the whole
    // native start, which is seconds for a large transcript.
    let (held, holding) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let holder = {
        let service = service.clone();
        tokio::spawn(async move {
            let _guard = service.session_lifecycle.lock().await;
            let _ = held.send(());
            let _ = released.await;
        })
    };
    holding.await.unwrap();

    // A bounded wait, not a duration threshold: an open that takes the lock
    // cannot finish until this test releases it.
    let opened = tokio::time::timeout(
        Duration::from_secs(5),
        open_session(&service, "warm".into()),
    )
    .await
    .expect("a warm open waited for another session's native start");
    assert_eq!(opened.0, ReplyKind::Success);
    assert_eq!(opened.1["sessionId"], json!("warm"));

    let _ = release.send(());
    holder.await.unwrap();
    service.shutdown().await;
    live.await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

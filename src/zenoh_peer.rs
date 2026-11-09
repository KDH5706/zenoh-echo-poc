use std::error::Error;
use zenoh::Config;

pub async fn connect_direct(
    port: &str,
    baud: u32,
) -> Result<zenoh::Session, Box<dyn Error + Send + Sync>> {

    if port.trim().is_empty() {
        return Err("Serial port is empty".into());
    }

    if baud == 0 {
        return Err("Invalid baud rate".into());
    }

    let mut config = Config::default();

    // 1. Zenoh Peer 모드
    config.insert_json5("mode", r#""peer""#)?;

    // 2. Serial Endpoint
    let endpoint = format!("serial/{}#baudrate={}", port, baud);

    // 3. Serial Listener 등록
    let endpoints = format!(r#"["{}"]"#, endpoint);

    config.insert_json5("listen/endpoints", &endpoints)?;

    // 4. Multicast Discovery 비활성화
    config.insert_json5("scouting/multicast/enabled", "false")?;

    println!("[ZENOH] Peer Mode");
    println!("[ZENOH] Serial Listener: {}", endpoint);

    // 5. Zenoh Session 생성
    let session = zenoh::open(config).await?;

    println!("[ZENOH] Serial Listener started");

    Ok(session)
}
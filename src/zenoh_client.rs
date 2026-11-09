use zenoh::Config;

// Zenoh Client -> RPi zenohd Router
pub async fn connect_router() -> Result<zenoh::Session, Box<dyn std::error::Error + Send + Sync>> {
    let mut config = Config::default();

    // Client 모드
    config.insert_json5("mode", r#""client""#)?;

    // RPi 로컬 Router 연결
    config.insert_json5("connect/endpoints", r#"["tcp/127.0.0.1:7447"]"#)?;

    // Multicast Discovery 비활성화
    config.insert_json5("scouting/multicast/enabled", "false")?;

    // Router 연결 실패 시 오류 반환
    config.insert_json5("connect/exit_on_failure", "true")?;

    println!("[ZENOH] Connecting to Router...");

    let session = zenoh::open(config).await?;

    println!("[ZENOH] Connected to Router!");

    Ok(session)
}

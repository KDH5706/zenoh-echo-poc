#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod zenoh_client;
mod zenoh_peer;
mod rtt_stats;

use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use slint::ComponentHandle;
use tokio::{runtime::Runtime, sync::watch, task::JoinHandle};
use rtt_stats::RttStats;
use zenoh::Session;

slint::include_modules!();

const ECHO_REQUEST: &str = "scrubber/echo/request";
const ECHO_REPLY: &str = "scrubber/echo/reply";
const MCU_DISCONNECT_REQUEST: &str = "scrubber/control/disconnect";

type PendingMap = HashMap<u64, Instant>;
const ECHO_TIMEOUT: Duration = Duration::from_secs(2);
const AUTO_INTERVAL: Duration = Duration::from_millis(100);
type SharedState = Arc<Mutex<AppState>>;
type ConnectResult = Result<Session, Box<dyn Error + Send + Sync>>;

struct Connection {
    session: Session,
    stop_tx: watch::Sender<bool>,
    rx_task: JoinHandle<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionMode {
    Router,
    #[default] Direct,
}

impl ConnectionMode {
    fn connected_label(self) -> &'static str {
        match self {
            Self::Router => "Connected to Router",
            Self::Direct => "Connected Direct",
        }
    }
}

#[derive(Default)]
struct AppState {
    connection: Option<Connection>,
    busy: bool,
    generation: u64,
    next_id: u64,
    pending: PendingMap,
    connection_mode: ConnectionMode,
    stats: RttStats,
    batch_running: bool,
}

fn append_log(ui: &AppWindow, message: &str) {
    let old = ui.get_log_text().to_string();
    let new_text = if old.is_empty() || old == "Waiting for communication..." {
        message.to_string()
    } else {
        format!("{old}\n{message}")
    };

    let lines: Vec<&str> = new_text.lines().collect();
    let start = lines.len().saturating_sub(300);
    ui.set_log_text(lines[start..].join("\n").into());
}

fn post_ui<F>(ui: slint::Weak<AppWindow>, f: F)
where
    F: FnOnce(AppWindow) + Send + 'static,
{
    let _ = ui.upgrade_in_event_loop(f);
}

async fn close_connection(mut conn: Connection) {
    println!("[Zenoh] Disconnect requested");

    // 1. RX Task에 종료 요청
    let _ = conn.stop_tx.send(true);

    // 2. RX Task 정상 종료 대기 (최대 2초)
    match tokio::time::timeout(Duration::from_secs(2), &mut conn.rx_task).await {
        Ok(result) => {
            println!("[Zenoh] RX task stopped: {:?}", result);
        }
        Err(_) => {
            eprintln!("[Zenoh] RX task timeout, aborting...");

            conn.rx_task.abort();
            let _ = conn.rx_task.await;
        }
    }

    // 3. Zenoh Session 종료
    println!("[Zenoh] Closing session...");

    let start = Instant::now();

    match conn.session.close().await {
        Ok(()) => {
            println!(
                "[Zenoh] Session closed successfully ({:?})",
                start.elapsed()
            );
        }

        Err(e) => {
            eprintln!(
                "[Zenoh] Session close failed after {:?}: {}",
                start.elapsed(),
                e
            );
        }
    }

    println!("[Zenoh] Disconnect finished");
}

async fn request_mcu_disconnect(session: &Session, ) -> Result<(), zenoh::Error> {
    session
    .put(MCU_DISCONNECT_REQUEST, "disconnect")
    .await?;

    Ok(())
}


fn update_statistics(ui: &AppWindow, stats: &RttStats) {
    let snap = stats.snapshot();
    let ms = |value: Option<f64>| -> slint::SharedString {
        value
            .map(|v| format!("{v:.2} ms"))
            .unwrap_or_else(|| "-- ms".to_string())
            .into()
    };

    ui.set_rtt_text(ms(snap.last));
    ui.set_avg_rtt(ms(snap.mean));
    ui.set_min_rtt(ms(snap.min));
    ui.set_max_rtt(ms(snap.max));
    ui.set_p50_rtt(ms(snap.p50));
    ui.set_p95_rtt(ms(snap.p95));
    ui.set_p99_rtt(ms(snap.p99));
    ui.set_stddev_rtt(ms(snap.stddev));
    ui.set_sample_count(snap.count.min(i32::MAX as usize) as i32);
    ui.set_timeout_count(stats.timeouts.min(i32::MAX as u64) as i32);
    ui.set_success_rate(format!("{:.1}%", stats.success_rate()).into());
}

async fn send_echo(
    session: Session,
    state: SharedState,
    ui_weak: slint::Weak<AppWindow>,
    generation: u64,
    message: String,
    quiet: bool,
) -> bool {
    let id = {
        let mut st = state.lock().unwrap();
        if st.generation != generation || st.connection.is_none() || st.busy {
            return false;
        }
        st.next_id += 1;
        let id = st.next_id;
        st.pending.insert(id, Instant::now());
        id
    };
    let payload = format!("{id}|{message}");
    match session.put(ECHO_REQUEST, payload).await {
        Ok(()) => {
            let state_ui = state.clone();
            post_ui(ui_weak, move |ui| {
                if state_ui.lock().unwrap().generation != generation {
                    return;
                }
                ui.set_tx_count(ui.get_tx_count() + 1);
                if !quiet {
                    append_log(&ui, &format!("[TX #{id}] {message}"));
                }
            });
            true
        }
        Err(e) => {
            {
                let mut st = state.lock().unwrap();
                if st.generation != generation {
                    return false;
                }
                st.pending.remove(&id);
                st.stats.send_errors += 1;
            }
            let state_ui = state.clone();
            post_ui(ui_weak, move |ui| {
                let st = state_ui.lock().unwrap();
                if st.generation != generation {
                    return;
                }
                update_statistics(&ui, &st.stats);
                drop(st);
                ui.set_error_count(ui.get_error_count() + 1);
                append_log(&ui, &format!("[ERROR] Echo TX failed: {e}"));
            });
            false
        }
    }
}


fn main() -> Result<(), Box<dyn Error>> {
    let ui = AppWindow::new()?;
    let rt = Arc::new(Runtime::new()?);
    let state: SharedState = Arc::new(Mutex::new(AppState::default()));

    // =====================================
    // CONNECT
    // =====================================
    {
        let ui_weak = ui.as_weak();
        let rt = rt.clone();
        let state = state.clone();

        ui.on_connect_requested(move |port, baud| {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };

            // Capture the mode atomically with the connection generation.
            let (generation, connection_mode) = {
                let mut st = state.lock().unwrap();
                if st.busy || st.connection.is_some() {
                    append_log(&ui, "[WARN] Connection is already active or busy");
                    return;
                }
                st.busy = true;
                st.generation += 1;
                st.pending.clear();
                (st.generation, st.connection_mode)
            };

            ui.set_session_status("Connecting...".into());
            match connection_mode {
                ConnectionMode::Router => append_log(
                    &ui,
                    "[INFO] Connecting to zenohd Router (TCP)",
                ),
                ConnectionMode::Direct => append_log(
                    &ui,
                    &format!(
                        "[INFO] Connecting Direct (GUI serial selection: {port}, {baud}; no zenohd)"
                    ),
                ),
            }

            let ui_weak = ui_weak.clone();
            let state = state.clone();
            rt.spawn(async move {
                let result: ConnectResult = match connection_mode {
                    ConnectionMode::Router => zenoh_client::connect_router().await,
                    ConnectionMode::Direct => {
                        // Slint SharedString → Rust String
                        let port = port.to_string();
                        match baud.as_str().trim().parse::<u32>() {
                            Ok(value) if value > 0 => zenoh_peer::connect_direct(&port, value).await,
                            _ => Err("Invalid baudrate".into()),
                        }
                    },
                };

                match result {
                    Ok(session) => {
                        // A stale connection attempt must not register subscribers.
                        let current = {
                            let st = state.lock().unwrap();
                            st.generation == generation && st.busy
                        };
                        if !current {
                            let _ = session.close().await;
                            return;
                        }

                        match session.declare_subscriber(ECHO_REPLY).await {
                            Ok(subscriber) => {
                                let (stop_tx, mut stop_rx) = watch::channel(false);
                                let rx_state = state.clone();
                                let rx_ui = ui_weak.clone();

                                let rx_task = tokio::spawn(async move {
                                    let mut sweep = tokio::time::interval(Duration::from_millis(100));
                                    loop {
                                        tokio::select! {
                                            _ = sweep.tick() => {
                                                let (expired, snapshot) = {
                                                    let mut st = rx_state.lock().unwrap();
                                                    if st.generation != generation { break; }
                                                    let before = st.pending.len();
                                                    st.pending.retain(|_, at| at.elapsed() < ECHO_TIMEOUT);
                                                    let expired = before - st.pending.len();
                                                    st.stats.timeouts += expired as u64;
                                                    (expired, st.stats.clone())
                                                };
                                                if expired > 0 {
                                                    let state_ui = rx_state.clone();
                                                    post_ui(rx_ui.clone(), move |ui| {
                                                        if state_ui.lock().unwrap().generation != generation { return; }
                                                        update_statistics(&ui, &snapshot);
                                                        ui.set_error_count(ui.get_error_count() + expired as i32);
                                                        append_log(&ui, &format!("[TIMEOUT] {expired} request(s) exceeded 2s"));
                                                    });
                                                }
                                            }
                                            changed = stop_rx.changed() => {
                                                if changed.is_err() || *stop_rx.borrow() {
                                                    break;
                                                }
                                            }
                                            sample = subscriber.recv_async() => {
                                                let Ok(sample) = sample else {
                                                    break;
                                                };

                                                let payload = sample.payload().to_bytes();
                                                let text = String::from_utf8_lossy(payload.as_ref()).to_string();
                                                let (id, content) = match text.split_once('|') {
                                                    Some((id, msg)) => {
                                                        (id.parse::<u64>().ok(), msg.to_string())
                                                    }
                                                    None => (None, text),
                                                };

                                                let (rtt, snapshot) = {
                                                    let mut st = rx_state.lock().unwrap();
                                                    if st.generation != generation { break; }
                                                    let rtt = id.and_then(|id| st.pending.remove(&id))
                                                        .map(|start| start.elapsed());
                                                    if let Some(rtt) = rtt {
                                                        st.stats.record(rtt);
                                                    }
                                                    (rtt, st.stats.clone())
                                                };

                                                let ui_state = rx_state.clone();
                                                post_ui(rx_ui.clone(), move |ui| {
                                                    if ui_state.lock().unwrap().generation != generation {
                                                        return;
                                                    }
                                                    ui.set_rx_count(ui.get_rx_count() + 1);
                                                    if !ui.get_batch_running() {
                                                        append_log(&ui, &format!("[RX] {content}"));
                                                    }
                                                    if rtt.is_some() { update_statistics(&ui, &snapshot); }
                                                });
                                            }
                                        }
                                    }
                                });

                                let connection = Connection { session, stop_tx, rx_task };
                                let mut maybe_connection = Some(connection);
                                let accepted = {
                                    let mut st = state.lock().unwrap();
                                    if st.generation == generation && st.busy {
                                        st.connection = maybe_connection.take();
                                        st.busy = false;
                                        true
                                    } else {
                                        false
                                    }
                                };

                                if accepted {
                                    let ui_state = state.clone();
                                    post_ui(ui_weak.clone(), move |ui| {
                                        let st = ui_state.lock().unwrap();
                                        if st.generation != generation || st.connection.is_none() {
                                            return;
                                        }
                                        drop(st);
                                        ui.set_connected(true);
                                        ui.set_session_status(connection_mode.connected_label().into());
                                        append_log(&ui, "[INFO] Zenoh session established");
                                    });
                                } else if let Some(connection) = maybe_connection {
                                    close_connection(connection).await;
                                }
                            }
                            Err(e) => {
                                let _ = session.close().await;
                                let is_current = {
                                    let mut st = state.lock().unwrap();
                                    if st.generation == generation {
                                        st.busy = false;
                                        true
                                    } else {
                                        false
                                    }
                                };
                                if is_current {
                                    let msg = format!("[ERROR] Subscriber failed: {e}");
                                    let ui_state = state.clone();
                                    post_ui(ui_weak.clone(), move |ui| {
                                        if ui_state.lock().unwrap().generation != generation {
                                            return;
                                        }
                                        ui.set_connected(false);
                                        ui.set_session_status("Connection failed".into());
                                        ui.set_error_count(ui.get_error_count() + 1);
                                        append_log(&ui, &msg);
                                    });
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let is_current = {
                            let mut st = state.lock().unwrap();
                            if st.generation == generation {
                                st.busy = false;
                                true
                            } else {
                                false
                            }
                        };
                        if is_current {
                            let msg = format!("[ERROR] {:?} connection failed: {e}", connection_mode);
                            let ui_state = state.clone();
                            post_ui(ui_weak.clone(), move |ui| {
                                if ui_state.lock().unwrap().generation != generation {
                                    return;
                                }
                                ui.set_connected(false);
                                ui.set_session_status("Connection failed".into());
                                ui.set_error_count(ui.get_error_count() + 1);
                                append_log(&ui, &msg);
                            });
                        }
                    }
                }
            });
        });
    }

    // =====================================
    // DISCONNECT
    // =====================================
    {
        let ui_weak = ui.as_weak();
        let rt = rt.clone();
        let state = state.clone();

        ui.on_disconnect_requested(move || {
            let (conn, generation) = {
                let mut st = state.lock().unwrap();
                if st.busy || st.connection.is_none() {
                    return;
                }
                st.busy = true;
                st.generation += 1;
                st.pending.clear();
                let generation = st.generation;
                (st.connection.take(), generation)
            };

            if let Some(ui) = ui_weak.upgrade() {
                ui.set_session_status("Disconnecting...".into());
            }

            let ui_weak = ui_weak.clone();
            let state = state.clone();
            rt.spawn(async move {
                if let Some(conn) = conn {
                    close_connection(conn).await;
                }
                {
                    let mut st = state.lock().unwrap();
                    if st.generation != generation {
                        return;
                    }
                    st.busy = false;
                }
                let ui_state = state.clone();
                post_ui(ui_weak, move |ui| {
                    if ui_state.lock().unwrap().generation != generation {
                        return;
                    }
                    ui.set_connected(false);
                    ui.set_session_status("Disconnected".into());
                    append_log(&ui, "[INFO] Zenoh session closed");
                });
            });
        });
    }

    // =====================================
    // MCU DISCONNECT
    // =====================================
    {
        let ui_weak = ui.as_weak();
        let rt = rt.clone();
        let state = state.clone();

        ui.on_mcu_disconnect_requested(move || {
            let (session, generation) = {
                let st = state.lock().unwrap();

                // 연결 중이거나 종료 처리 중이면 요청하지 않음
                if st.busy || st.batch_running {
                    return;
                }

                let Some(session) = st
                    .connection
                    .as_ref()
                    .map(|conn| conn.session.clone())
                else {
                    return;
                };

                (session, st.generation)
            };

            let ui_weak = ui_weak.clone();
            let state = state.clone();

            rt.spawn(async move {
                let result = request_mcu_disconnect(&session).await;

                post_ui(ui_weak, move |ui| {
                    let st = state.lock().unwrap();

                    if st.generation != generation {
                        return;
                    }

                    drop(st);

                    match result {
                        Ok(()) => {
                            append_log(
                                &ui,
                                "[MCU] Disconnect request published",
                            );
                        }

                        Err(e) => {
                            ui.set_error_count(
                                ui.get_error_count() + 1
                            );

                            append_log(
                                &ui,
                                &format!(
                                    "[ERROR] MCU disconnect request failed: {e}"
                                ),
                            );
                        }
                    }
                });
            });
        });
    }


    // =====================================
    // SEND ECHO (manual)
    // =====================================
    {
        let ui_weak = ui.as_weak();
        let rt = rt.clone();
        let state = state.clone();

        ui.on_send_requested(move |message| {
            let message = message.to_string();
            let (session, id, generation) = {
                let mut st = state.lock().unwrap();
                if st.busy {
                    return;
                }
                let Some(session) = st.connection.as_ref().map(|c| c.session.clone()) else {
                    return;
                };
                st.next_id += 1;
                let id = st.next_id;
                let generation = st.generation;
                st.pending.insert(id, Instant::now());
                (session, id, generation)
            };

            let payload = format!("{id}|{message}");
            let ui_weak = ui_weak.clone();
            let state = state.clone();

            rt.spawn(async move {
                match session.put(ECHO_REQUEST, payload).await {
                    Ok(()) => {
                        let ui_state = state.clone();
                        post_ui(ui_weak, move |ui| {
                            if ui_state.lock().unwrap().generation != generation {
                                return;
                            }
                            ui.set_tx_count(ui.get_tx_count() + 1);
                            append_log(&ui, &format!("[TX #{id}] {message}"));
                        });
                    }
                    Err(e) => {
                        let is_current = {
                            let mut st = state.lock().unwrap();
                            if st.generation == generation {
                                st.pending.remove(&id);
                                true
                            } else {
                                false
                            }
                        };
                        if is_current {
                            let msg = format!("[ERROR] Echo TX failed: {e}");
                            let ui_state = state.clone();
                            post_ui(ui_weak, move |ui| {
                                if ui_state.lock().unwrap().generation != generation {
                                    return;
                                }
                                ui.set_error_count(ui.get_error_count() + 1);
                                append_log(&ui, &msg);
                            });
                        }
                    }
                }
            });
        });
    }
    

    // =====================================
    // AUTO BENCHMARK: sequential requests with fixed spacing
    // =====================================
    {
        let ui_weak = ui.as_weak();
        let rt = rt.clone();
        let state = state.clone();
        ui.on_start_batch_requested(move |count, message| {
            if !(1..=10000).contains(&count) { return; }
            let (session, generation) = {
                let mut st = state.lock().unwrap();
                if st.busy || st.batch_running { return; }
                let Some(session) = st.connection.as_ref().map(|c| c.session.clone()) else { return; };
                st.batch_running = true;
                (session, st.generation)
            };
            if let Some(ui) = ui_weak.upgrade() {
                ui.set_batch_running(true);
                ui.set_batch_progress(format!("0 / {count}").into());
                append_log(&ui, &format!("[BENCH] Starting {count} Echo requests (100ms interval)"));
            }
            let state = state.clone();
            let ui_weak = ui_weak.clone();
            let message = message.to_string();
            rt.spawn(async move {
                let mut sent = 0;
                for n in 0..count {
                    let running = {
                        let st = state.lock().unwrap();
                        st.generation == generation && st.batch_running && st.connection.is_some()
                    };
                    if !running { break; }
                    if !send_echo(session.clone(), state.clone(), ui_weak.clone(), generation, message.clone(), true).await { break; }
                    sent += 1;
                    if n % 10 == 0 || n + 1 == count {
                        let state_ui = state.clone();
                        post_ui(ui_weak.clone(), move |ui| {
                            if state_ui.lock().unwrap().generation == generation {
                                ui.set_batch_progress(format!("{sent} / {count}").into());
                            }
                        });
                    }
                    tokio::time::sleep(AUTO_INTERVAL).await;
                }
                let current = {
                    let mut st = state.lock().unwrap();
                    if st.generation != generation { false } else {
                        st.batch_running = false;
                        true
                    }
                };
                if current {
                    let state_ui = state.clone();
                    post_ui(ui_weak, move |ui| {
                        if state_ui.lock().unwrap().generation != generation { return; }
                        ui.set_batch_running(false);
                        append_log(&ui, &format!("[BENCH] Finished sending {sent}/{count}; late replies/timeouts may still arrive"));
                    });
                }
            });
        });
    }
    {
        let ui_weak = ui.as_weak();
        let state = state.clone();
        ui.on_stop_batch_requested(move || {
            let mut st = state.lock().unwrap();
            st.batch_running = false;
            drop(st);
            if let Some(ui) = ui_weak.upgrade() {
                ui.set_batch_running(false);
                append_log(&ui, "[BENCH] Stop requested");
            }
        });
    }
    {
        let ui_weak = ui.as_weak();
        let state = state.clone();
        ui.on_reset_statistics_requested(move || {
            let mut st = state.lock().unwrap();
            if st.batch_running {
                return;
            }
            st.stats = RttStats::default();
            st.pending.clear();
            let snapshot = st.stats.clone();
            drop(st);
            if let Some(ui) = ui_weak.upgrade() {
                update_statistics(&ui, &snapshot);
                ui.set_tx_count(0);
                ui.set_rx_count(0);
                ui.set_error_count(0);
                ui.set_batch_progress("0 / 0".into());
                append_log(&ui, "[INFO] RTT statistics reset");
            }
        });
    }

    // =====================================
    // CLEAR LOG
    // =====================================
    {
        let ui_weak = ui.as_weak();
        ui.on_clear_log_requested(move || {
            if let Some(ui) = ui_weak.upgrade() {
                ui.set_log_text("".into());
            }
        });
    }

    // =====================================
    // MODE CHANGED
    // =====================================
    {
        let ui_weak = ui.as_weak();
        let state = state.clone();

        ui.on_mode_changed(move |mode| {
            let selected_mode = match mode.as_str() {
                "Peer" | "Direct" => ConnectionMode::Direct,
                "Client" | "Router" => ConnectionMode::Router,
                other => {
                    if let Some(ui) = ui_weak.upgrade() {
                        append_log(&ui, &format!("[WARN] Unknown mode: {other}"));
                    }
                    return;
                }
            };

            let changed = {
                let mut st = state.lock().unwrap();
                if st.busy || st.connection.is_some() {
                    false
                } else {
                    st.connection_mode = selected_mode;
                    true
                }
            };

            if let Some(ui) = ui_weak.upgrade() {
                if changed {
                    append_log(&ui, &format!("[INFO] Selected mode: {:?}", selected_mode));
                } else {
                    append_log(&ui, "[WARN] Disconnect before changing mode");
                    // If the Slint selector has already changed, restore its selection
                    // through the appropriate Slint setter in your UI definition.
                }
            }
        });
    }

    ui.run()?;

    // GUI shutdown: release the session before stopping Tokio.
    let conn = {
        let mut st = state.lock().unwrap();
        st.generation += 1;
        st.connection.take()
    };
    if let Some(conn) = conn {
        rt.block_on(close_connection(conn));
    }

    Ok(())
}

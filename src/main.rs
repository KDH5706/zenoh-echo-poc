#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod zenoh_client;
mod zenoh_peer;

use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
    time::Instant,
};

use slint::ComponentHandle;
use tokio::{runtime::Runtime, sync::watch, task::JoinHandle};
use zenoh::Session;

slint::include_modules!();

const ECHO_REQUEST: &str = "scrubber/echo/request";
const ECHO_REPLY: &str = "scrubber/echo/reply";

type PendingMap = HashMap<u64, Instant>;
type SharedState = Arc<Mutex<AppState>>;
type ConnectResult = Result<Session, Box<dyn Error + Send + Sync>>;

struct Connection {
    session: Session,
    stop_tx: watch::Sender<bool>,
    rx_task: JoinHandle<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionMode {
    #[default]
    Router,
    Direct,
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

async fn close_connection(conn: Connection) {
    let _ = conn.stop_tx.send(true);
    conn.rx_task.abort();
    let _ = conn.rx_task.await;
    if let Err(e) = conn.session.close().await {
        eprintln!("Zenoh close error: {e}");
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

                        // Slint SharedString → Rust u32
                        let baud: u32 = match baud.as_str().trim().parse() {
                            Ok(value) if value > 0 => value,
                            _ => {
                                if let Some(ui) = ui_weak.upgrade() {
                                    append_log(&ui, "[ERROR] Invalid baudrate");
                                }
                                return;
                            }
                        };

                        zenoh_peer::connect_direct(&port, baud).await
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
                                    loop {
                                        tokio::select! {
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

                                                let rtt = {
                                                    let mut st = rx_state.lock().unwrap();
                                                    if st.generation != generation {
                                                        break;
                                                    }
                                                    id.and_then(|id| st.pending.remove(&id))
                                                        .map(|start| start.elapsed())
                                                };

                                                let ui_state = rx_state.clone();
                                                post_ui(rx_ui.clone(), move |ui| {
                                                    if ui_state.lock().unwrap().generation != generation {
                                                        return;
                                                    }
                                                    ui.set_rx_count(ui.get_rx_count() + 1);
                                                    append_log(&ui, &format!("[RX] {content}"));
                                                    if let Some(rtt) = rtt {
                                                        ui.set_rtt_text(
                                                            format!("{:.2} ms", rtt.as_secs_f64() * 1000.0).into()
                                                        );
                                                    }
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
    // SEND ECHO
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

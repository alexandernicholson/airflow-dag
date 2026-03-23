use std::net::SocketAddr;
use std::sync::Arc;

use rebar::DistributedRuntime;
use rebar_cluster::connection::manager::{ConnectionManager, TransportConnector};
use rebar_cluster::transport::tcp::TcpTransport;
use rebar_cluster::transport::{TransportConnection, TransportError, TransportListener};

/// TCP connector implementing Rebar's `TransportConnector` trait.
pub struct TcpConnector;

#[async_trait::async_trait]
impl TransportConnector for TcpConnector {
    async fn connect(
        &self,
        addr: SocketAddr,
    ) -> Result<Box<dyn TransportConnection>, TransportError> {
        let transport = TcpTransport::new();
        let conn = transport.connect(addr).await?;
        Ok(Box::new(conn))
    }
}

/// Create a `ConnectionManager` with TCP transport.
pub fn tcp_connection_manager() -> ConnectionManager {
    ConnectionManager::new(Box::new(TcpConnector))
}

/// Create a `DistributedRuntime` with TCP transport.
pub fn create_runtime(node_id: u64) -> DistributedRuntime {
    DistributedRuntime::new(node_id, tcp_connection_manager())
}

/// Start a TCP listener that accepts connections and delivers inbound
/// frames to the local process table.
pub async fn start_listener(
    table: Arc<rebar::process::table::ProcessTable>,
    addr: SocketAddr,
) -> SocketAddr {
    let transport = TcpTransport::new();
    let listener = transport.listen(addr).await.expect("failed to bind listener");
    let actual_addr = listener.local_addr();

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok(mut conn) => {
                    let table = Arc::clone(&table);
                    tokio::spawn(async move {
                        while let Ok(frame) = conn.recv().await {
                            let _ = rebar_cluster::router::deliver_inbound_frame(&table, &frame);
                        }
                    });
                }
                Err(_) => break,
            }
        }
    });

    actual_addr
}

/// Start a background task that pumps outbound messages from the
/// `DistributedRuntime`'s router queue to the network via `ConnectionManager`.
///
/// Returns a handle that can be used to stop the pump.
pub fn start_outbound_pump(
    drt: Arc<tokio::sync::Mutex<DistributedRuntime>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let processed = {
                let mut rt = drt.lock().await;
                rt.process_outbound().await
            };
            if !processed {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }
    })
}

/// Connect to a peer node with retries.
pub async fn connect_to_peer(
    drt: &mut DistributedRuntime,
    node_id: u64,
    addr: SocketAddr,
    max_retries: u32,
) -> Result<(), String> {
    for attempt in 0..max_retries {
        match drt.connection_manager_mut().connect(node_id, addr).await {
            Ok(()) => {
                println!("  Connected to node {node_id} at {addr}");
                return Ok(());
            }
            Err(e) => {
                if attempt + 1 < max_retries {
                    let delay = std::time::Duration::from_millis(500 * u64::from(attempt + 1));
                    println!("  Retry {}/{max_retries} connecting to node {node_id}: {e}", attempt + 1);
                    tokio::time::sleep(delay).await;
                } else {
                    return Err(format!("failed to connect to node {node_id} after {max_retries} attempts: {e}"));
                }
            }
        }
    }
    unreachable!()
}

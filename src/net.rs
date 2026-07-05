use anyhow::Result;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;
use tokio::net::TcpSocket;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Endpoint};

/// Binds a listener for a gRPC server and returns it with the actual bound
/// address. Port 0 requests an ephemeral port; the returned address always
/// carries the real one. `SO_REUSEADDR` is set so a restarted node can
/// rebind its address while connections from its previous life drain.
pub fn bind_server(addr: &str) -> Result<(TcpIncoming, String)> {
    let addr: SocketAddr = addr.parse()?;
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    let listener = socket.listen(1024)?;
    let local_addr = listener.local_addr()?.to_string();
    let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));
    Ok((incoming, local_addr))
}

/// Cache of lazily-connected gRPC channels keyed by `host:port`.
///
/// A tonic [`Channel`] multiplexes concurrent requests over a single HTTP/2
/// connection and transparently reconnects after transport failures, so one
/// cached channel per peer replaces a fresh TCP + HTTP/2 handshake per RPC.
/// Channels connect lazily: dialing failures surface as `UNAVAILABLE` on the
/// request that triggered them and the next request retries the connection.
#[derive(Default)]
pub struct ChannelCache {
    channels: RwLock<HashMap<String, Channel>>,
}

impl ChannelCache {
    pub fn get(&self, addr: &str) -> Result<Channel> {
        {
            let channels = self.channels.read().expect("channel cache lock poisoned");
            if let Some(channel) = channels.get(addr) {
                return Ok(channel.clone());
            }
        }
        let endpoint = Endpoint::from_shared(format!("http://{addr}"))?;
        let mut channels = self.channels.write().expect("channel cache lock poisoned");
        Ok(channels
            .entry(addr.to_string())
            .or_insert_with(|| endpoint.connect_lazy())
            .clone())
    }
}

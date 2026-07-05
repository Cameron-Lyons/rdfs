use anyhow::Result;
use std::collections::HashMap;
use std::sync::RwLock;
use tonic::transport::{Channel, Endpoint};

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

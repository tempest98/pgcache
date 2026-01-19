use tokio::sync::{mpsc::Sender, watch};

use crate::cache::ProxyMessage;

pub type CacheSenderInner = Sender<ProxyMessage>;

/// Error returned when sending to the cache fails.
pub enum CacheSendError {
    /// Cache is temporarily unavailable (e.g., during restart).
    CacheUnavailable(ProxyMessage),
    /// The underlying channel is closed.
    ChannelClosed(ProxyMessage),
}

impl CacheSendError {
    /// Extracts the original message from the error.
    pub fn into_message(self) -> ProxyMessage {
        let (CacheSendError::CacheUnavailable(msg) | CacheSendError::ChannelClosed(msg)) = self;
        msg
    }
}

/// Wrapper around `watch::Receiver` for connections to use.
///
/// Connections hold a `CacheSender` and call `send()` on each cache operation.
/// The sender automatically sees updated cache channels after restart.
#[derive(Clone)]
pub struct CacheSender {
    rx: watch::Receiver<Option<CacheSenderInner>>,
}

impl CacheSender {
    /// Sends a message to the cache, returning error if cache unavailable.
    pub async fn send(&self, msg: ProxyMessage) -> Result<(), CacheSendError> {
        // Borrow the current sender from the watch channel
        let maybe_sender = self.rx.borrow().clone();

        match maybe_sender {
            Some(sender) => sender
                .send(msg)
                .await
                .map_err(|e| CacheSendError::ChannelClosed(e.0)),
            None => Err(CacheSendError::CacheUnavailable(msg)),
        }
    }
}

/// Server-side updater for the watch channel.
///
/// The server holds this and calls `sender_update()` on successful restart
/// or `sender_clear()` when the cache exits.
pub struct CacheSenderUpdater {
    tx: watch::Sender<Option<CacheSenderInner>>,
}

impl CacheSenderUpdater {
    /// Creates a new updater and initial subscriber.
    pub fn new(initial: CacheSenderInner) -> (Self, CacheSender) {
        let (tx, rx) = watch::channel(Some(initial));
        (Self { tx }, CacheSender { rx })
    }

    /// Updates all subscribers with a new cache sender (called on successful restart).
    pub fn sender_update(&self, new: CacheSenderInner) {
        // Ignore send error - if no receivers, nothing to update
        let _ = self.tx.send(Some(new));
    }

    /// Clears the cache sender, marking cache as unavailable (called on cache exit).
    pub fn sender_clear(&self) {
        let _ = self.tx.send(None);
    }

    /// Creates a new subscriber for a new worker.
    pub fn sender_subscribe(&self) -> CacheSender {
        CacheSender {
            rx: self.tx.subscribe(),
        }
    }
}

//! A bus type (SPMC) channel where multiple consumers can subscribe to single source channel.
//! The bus can internally use any channel/receiver combination. By default it provides constructor
//! methods to use the `futures::channel::mpsc` channels.

#![deny(missing_docs, unused_must_use, unused_mut, unused_imports, unused_import_braces)]

use std::pin::Pin;
use std::sync::{Arc, Weak};

use futures_channel::mpsc;
use futures_core::task::{Context, Poll};
use futures_core::Stream;
use futures_sink::Sink;
use parking_lot::RwLock;
use slab::Slab;

/// The struct containing references to the receivers and at the same time the data source for the
/// bus.
pub struct FutureBus<T, S, R>
where
    T: Send + Clone + 'static,
    S: Sink<T> + Unpin,
    R: Stream<Item = T> + Unpin,
{
    /// the senders pointing to all the registered subscribers
    senders: Arc<RwLock<Slab<S>>>,
    /// factory function to create a new instance of the underlying channel
    ctor: Arc<dyn Fn() -> (S, R) + Send + Sync>,
}

/// A receiver of data from a [`FutureBus`](self::FutureBus). When it is dropped, the sender is
/// removed from the bus. Note that this means dropping a receiver causes a write access to the
/// bus, so it causes a lock.
pub struct BusSubscriber<T, S, R>
where
    T: Send + Clone + 'static,
    S: Sink<T> + Unpin,
    R: Stream<Item = T> + Unpin,
{
    inner_receiver: R,
    sender_registry: Weak<RwLock<Slab<S>>>,
    sender_key: usize,
    /// factory function to create a new instance of the underlying channel
    ctor: Arc<dyn Fn() -> (S, R) + Send + Sync>,
}

impl<T, S, R> Stream for BusSubscriber<T, S, R>
where
    T: Send + Clone + 'static,
    S: Sink<T> + Unpin,
    R: Stream<Item = T> + Unpin,
{
    type Item = R::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut Pin::into_inner(self).inner_receiver).poll_next(cx)
    }
}

impl<T, S, R> Drop for BusSubscriber<T, S, R>
    where
        T: Send + Clone + 'static,
        S: Sink<T> + Unpin,
        R: Stream<Item = T> + Unpin,
{
    fn drop(&mut self) {
        if let Some(senders) = self.sender_registry.upgrade() {
            senders.write().remove(self.sender_key);
        }
    }
}

impl<T, S, R> BusSubscriber<T, S, R>
where
    T: Send + Clone + 'static,
    S: Sink<T> + Unpin,
    R: Stream<Item = T> + Unpin,
{
    /// Attempts to create a new subscriber. Returns `None` if the underlying bus has
    /// since been dropped.
    pub fn try_clone(&self) -> Option<Self> {
        self.sender_registry.upgrade().and_then(|senders| {
            let (sender, receiver) = self.ctor.as_ref()();
            let key = senders.write().insert(sender);
            Some(BusSubscriber {
                inner_receiver: receiver,
                sender_registry: self.sender_registry.clone(),
                sender_key: key,
                ctor: self.ctor.clone()
            })
        })
    }
}

impl<T, S, R> FutureBus<T, S, R>
where
    T: Send + Clone + 'static,
    S: Sink<T> + Unpin,
    R: Stream<Item = T> + Unpin,
{
    /// Create a new subscriber channel
    pub fn subscribe(&mut self) -> BusSubscriber<T, S, R> {
        let (sender, receiver) = self.ctor.as_ref()();
        let key = self.senders.write().insert(sender);
        BusSubscriber {
            inner_receiver: receiver,
            sender_registry: Arc::downgrade(&self.senders),
            sender_key: key,
            ctor: self.ctor.clone()
        }
    }
}

/// Create a new bus using bounded channels
pub fn bounded<T: Send + Clone + 'static>(
    buffer: usize,
) -> FutureBus<T, mpsc::Sender<T>, mpsc::Receiver<T>> {
    FutureBus {
        senders: Arc::new(RwLock::new(Slab::new())),
        ctor: Arc::new(move || mpsc::channel::<T>(buffer)),
    }
}

/// Create a new bus using unbounded channels
pub fn unbounded<T: Send + Clone + 'static>(
) -> FutureBus<T, mpsc::UnboundedSender<T>, mpsc::UnboundedReceiver<T>> {
    FutureBus {
        senders: Arc::new(RwLock::new(Slab::new())),
        ctor: Arc::new(mpsc::unbounded::<T>),
    }
}

impl<T, S, R> Sink<T> for FutureBus<T, S, R>
where
    T: Send + Clone + 'static,
    S: Sink<T> + Unpin,
    R: Stream<Item = T> + Unpin,
{
    type Error = S::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::into_inner(self)
            .senders
            .write()
            .iter_mut()
            .map(|(_, sender)| Pin::new(sender).poll_ready(cx))
            .find_map(|poll| match poll {
                Poll::Ready(Err(_)) | Poll::Pending => Some(poll),
                _ => None,
            })
            .or_else(|| Some(Poll::Ready(Ok(()))))
            .unwrap()
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let mut senders = Pin::into_inner(self)
            .senders
            .write();

        senders.iter_mut()
            .skip(1)
            .map(|(_, sender)| {
                Pin::new(sender).start_send(item.clone())
            })
            .collect::<Result<_, _>>()?;

        if let Some((_, first)) = senders.iter_mut().next() {
            Pin::new(first).start_send(item)?;
        }
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::into_inner(self)
            .senders
            .write()
            .iter_mut()
            .map(|(_, sender)| Pin::new(sender).poll_flush(cx))
            .find_map(|poll| match poll {
                Poll::Ready(Err(_)) | Poll::Pending => Some(poll),
                _ => None,
            })
            .or_else(|| Some(Poll::Ready(Ok(()))))
            .unwrap()
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::into_inner(self)
            .senders
            .write()
            .iter_mut()
            .map(|(_, sender)| Pin::new(sender).poll_close(cx))
            .find_map(|poll| match poll {
                Poll::Ready(Err(_)) | Poll::Pending => Some(poll),
                _ => None,
            })
            .or_else(|| Some(Poll::Ready(Ok(()))))
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use futures::{SinkExt, StreamExt};

    use crate::unbounded;

    #[test]
    fn test_subscribe() {
        let mut bus = unbounded();
        let mut r1 = bus.subscribe();
        let mut r2 = r1.try_clone().unwrap();
        block_on(bus.send(10)).unwrap();
        assert_eq!(block_on(r1.next()), Some(10));
        assert_eq!(block_on(r2.next()), Some(10));
    }

    #[test]
    fn test_drop() {
        let mut bus = unbounded();
        let mut r1 = bus.subscribe();
        {
            let mut r2 = bus.subscribe();
            block_on(bus.send(10)).unwrap();
            assert_eq!(block_on(r1.next()), Some(10));
            assert_eq!(block_on(r2.next()), Some(10));
        }
        block_on(bus.send(15)).unwrap();
        assert_eq!(block_on(r1.next()), Some(15));
    }
}

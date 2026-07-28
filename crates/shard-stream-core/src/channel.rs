use std::error::Error;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{
    Receiver, RecvError, RecvTimeoutError, SyncSender, TryRecvError,
    TrySendError as StdTrySendError, sync_channel,
};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelConfigError {
    ZeroSlots,
    ZeroBytes,
}

impl fmt::Display for ChannelConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroSlots => formatter.write_str("channel slot capacity must be nonzero"),
            Self::ZeroBytes => formatter.write_str("channel byte capacity must be nonzero"),
        }
    }
}

impl Error for ChannelConfigError {}

#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    TooLarge {
        item: T,
        bytes: usize,
        max_bytes: usize,
    },
    ByteBudget {
        item: T,
        bytes: usize,
        available_bytes: usize,
    },
    Full(T),
    Disconnected(T),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelStats {
    pub used_bytes: usize,
    pub max_bytes: usize,
}

#[derive(Debug)]
struct ByteBudget {
    used: AtomicUsize,
    max: usize,
}

impl ByteBudget {
    fn try_reserve(&self, bytes: usize) -> Result<(), ReserveError> {
        if bytes > self.max {
            return Err(ReserveError::TooLarge);
        }
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.max)
            })
            .map(|_| ())
            .map_err(|_| ReserveError::Unavailable)
    }

    fn release(&self, bytes: usize) {
        let previous = self.used.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(previous >= bytes, "channel byte budget underflow");
    }

    fn stats(&self) -> ChannelStats {
        ChannelStats {
            used_bytes: self.used.load(Ordering::Acquire),
            max_bytes: self.max,
        }
    }
}

enum ReserveError {
    TooLarge,
    Unavailable,
}

#[derive(Debug)]
pub struct Budgeted<T> {
    item: Option<T>,
    charged_bytes: usize,
    budget: Arc<ByteBudget>,
}

impl<T> Budgeted<T> {
    fn new(item: T, charged_bytes: usize, budget: Arc<ByteBudget>) -> Self {
        Self {
            item: Some(item),
            charged_bytes,
            budget,
        }
    }

    #[must_use]
    pub const fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }

    pub fn into_inner(mut self) -> T {
        self.item.take().expect("budgeted item is present")
    }

    pub fn try_map<U, E>(mut self, map: impl FnOnce(T) -> Result<U, E>) -> Result<Budgeted<U>, E> {
        let item = self.item.take().expect("budgeted item is present");
        let mapped = map(item)?;
        let charged_bytes = std::mem::replace(&mut self.charged_bytes, 0);
        Ok(Budgeted::new(
            mapped,
            charged_bytes,
            Arc::clone(&self.budget),
        ))
    }
}

impl<T> Deref for Budgeted<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.item.as_ref().expect("budgeted item is present")
    }
}

impl<T> Drop for Budgeted<T> {
    fn drop(&mut self) {
        self.budget.release(self.charged_bytes);
    }
}

#[derive(Debug)]
pub struct ByteBoundedSender<T> {
    sender: SyncSender<Budgeted<T>>,
    budget: Arc<ByteBudget>,
}

impl<T> Clone for ByteBoundedSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            budget: Arc::clone(&self.budget),
        }
    }
}

impl<T> ByteBoundedSender<T> {
    pub fn try_send(&self, item: T, bytes: usize) -> Result<(), TrySendError<T>> {
        if let Err(error) = self.budget.try_reserve(bytes) {
            return Err(match error {
                ReserveError::TooLarge => TrySendError::TooLarge {
                    item,
                    bytes,
                    max_bytes: self.budget.max,
                },
                ReserveError::Unavailable => {
                    let used = self.budget.used.load(Ordering::Acquire);
                    TrySendError::ByteBudget {
                        item,
                        bytes,
                        available_bytes: self.budget.max.saturating_sub(used),
                    }
                }
            });
        }

        let budgeted = Budgeted::new(item, bytes, Arc::clone(&self.budget));
        self.sender.try_send(budgeted).map_err(|error| match error {
            StdTrySendError::Full(item) => TrySendError::Full(item.into_inner()),
            StdTrySendError::Disconnected(item) => TrySendError::Disconnected(item.into_inner()),
        })
    }

    #[must_use]
    pub fn stats(&self) -> ChannelStats {
        self.budget.stats()
    }
}

#[derive(Debug)]
pub struct ByteBoundedReceiver<T> {
    receiver: Receiver<Budgeted<T>>,
    budget: Arc<ByteBudget>,
}

impl<T> ByteBoundedReceiver<T> {
    pub fn recv(&self) -> Result<Budgeted<T>, RecvError> {
        self.receiver.recv()
    }

    pub fn try_recv(&self) -> Result<Budgeted<T>, TryRecvError> {
        self.receiver.try_recv()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<Budgeted<T>, RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }

    #[must_use]
    pub fn stats(&self) -> ChannelStats {
        self.budget.stats()
    }
}

pub fn byte_bounded_channel<T>(
    max_slots: usize,
    max_bytes: usize,
) -> Result<(ByteBoundedSender<T>, ByteBoundedReceiver<T>), ChannelConfigError> {
    if max_slots == 0 {
        return Err(ChannelConfigError::ZeroSlots);
    }
    if max_bytes == 0 {
        return Err(ChannelConfigError::ZeroBytes);
    }

    let (sender, receiver) = sync_channel(max_slots);
    let budget = Arc::new(ByteBudget {
        used: AtomicUsize::new(0),
        max: max_bytes,
    });
    Ok((
        ByteBoundedSender {
            sender,
            budget: Arc::clone(&budget),
        },
        ByteBoundedReceiver { receiver, budget },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_both_capacity_dimensions() {
        assert!(matches!(
            byte_bounded_channel::<u8>(0, 1),
            Err(ChannelConfigError::ZeroSlots)
        ));
        assert!(matches!(
            byte_bounded_channel::<u8>(1, 0),
            Err(ChannelConfigError::ZeroBytes)
        ));
    }

    #[test]
    fn byte_permit_lives_until_received_item_is_dropped() {
        let (sender, receiver) = byte_bounded_channel(2, 10).expect("channel");
        sender.try_send("first", 8).expect("first send");

        assert!(matches!(
            sender.try_send("second", 3),
            Err(TrySendError::ByteBudget { .. })
        ));

        let first = receiver.recv().expect("first receive");
        assert_eq!(sender.stats().used_bytes, 8);
        assert!(matches!(
            sender.try_send("second", 3),
            Err(TrySendError::ByteBudget { .. })
        ));

        drop(first);
        sender.try_send("second", 3).expect("permit released");
        assert_eq!(
            receiver.recv().expect("second receive").into_inner(),
            "second"
        );
        assert_eq!(sender.stats().used_bytes, 0);
    }

    #[test]
    fn slot_capacity_is_enforced_independently() {
        let (sender, receiver) = byte_bounded_channel(1, 100).expect("channel");
        sender.try_send(1, 1).expect("first send");
        assert_eq!(sender.try_send(2, 1), Err(TrySendError::Full(2)));
        assert_eq!(receiver.recv().expect("receive").into_inner(), 1);
        sender.try_send(2, 1).expect("slot released");
    }

    #[test]
    fn oversized_and_disconnected_sends_return_the_item() {
        let (sender, receiver) = byte_bounded_channel(1, 4).expect("channel");
        assert_eq!(
            sender.try_send(7, 5),
            Err(TrySendError::TooLarge {
                item: 7,
                bytes: 5,
                max_bytes: 4
            })
        );
        drop(receiver);
        assert_eq!(sender.try_send(8, 4), Err(TrySendError::Disconnected(8)));
        assert_eq!(sender.stats().used_bytes, 0);
    }
}

//! Shared engine lifetime only; neither variant grants serving authority.

use std::fmt;
use std::ops::Deref;
use std::sync::{Arc, Weak};

#[cfg(unix)]
use crate::OwnedRecoveredEngine;
use crate::StreamEngine;

// Keep the actual existing allocation. An extra Arc around this enum would
// give two handles over the same engine different receiver identities.
enum Owner {
    Ordinary(Arc<StreamEngine>),
    #[cfg(unix)]
    Recovered(Arc<OwnedRecoveredEngine>),
}

/// Opaque strong engine owner, including genuine recovered storage custody.
///
/// The ordinary variant retains the exact existing Arc. On Unix, the recovered
/// variant retains the actual `OwnedRecoveredEngine` through every clone and
/// its final physical sink/worker cleanup. Only a borrow of `StreamEngine` is
/// exposed. This cannot authenticate currentness, complete recovery, replace an
/// installed fence, or authorize serving. The host still owns those joins.
pub struct EngineHandle {
    owner: Owner,
}

impl From<Arc<StreamEngine>> for EngineHandle {
    fn from(engine: Arc<StreamEngine>) -> Self {
        Self {
            owner: Owner::Ordinary(engine),
        }
    }
}

impl EngineHandle {
    /// Retains the actual native recovery owner without extracting its engine.
    ///
    /// Native construction establishes storage custody only. The caller must
    /// separately complete its current-control, durable and startup contracts.
    #[cfg(unix)]
    pub fn from_recovered(engine: Arc<OwnedRecoveredEngine>) -> Self {
        Self {
            owner: Owner::Recovered(engine),
        }
    }

    /// Borrows the normal API while this actual strong owner remains retained.
    pub fn engine(&self) -> &StreamEngine {
        match &self.owner {
            Owner::Ordinary(engine) => engine.as_ref(),
            #[cfg(unix)]
            Owner::Recovered(engine) => engine.engine(),
        }
    }

    /// Weakly references the SAME underlying allocation, without a new owner.
    pub fn downgrade(&self) -> WeakEngineHandle {
        let owner = match &self.owner {
            Owner::Ordinary(engine) => WeakOwner::Ordinary(Arc::downgrade(engine)),
            #[cfg(unix)]
            Owner::Recovered(engine) => WeakOwner::Recovered(Arc::downgrade(engine)),
        };
        WeakEngineHandle { owner }
    }

    /// Exact allocation identity within the actual ordinary/recovered variant.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (&self.owner, &other.owner) {
            (Owner::Ordinary(left), Owner::Ordinary(right)) => Arc::ptr_eq(left, right),
            #[cfg(unix)]
            (Owner::Recovered(left), Owner::Recovered(right)) => Arc::ptr_eq(left, right),
            #[cfg(unix)]
            _ => false,
        }
    }
}

impl Clone for EngineHandle {
    fn clone(&self) -> Self {
        let owner = match &self.owner {
            Owner::Ordinary(engine) => Owner::Ordinary(Arc::clone(engine)),
            #[cfg(unix)]
            Owner::Recovered(engine) => Owner::Recovered(Arc::clone(engine)),
        };
        Self { owner }
    }
}
impl Deref for EngineHandle {
    type Target = StreamEngine;
    fn deref(&self) -> &Self::Target {
        self.engine()
    }
}
impl fmt::Debug for EngineHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EngineHandle").finish_non_exhaustive()
    }
}

enum WeakOwner {
    Ordinary(Weak<StreamEngine>),
    #[cfg(unix)]
    Recovered(Weak<OwnedRecoveredEngine>),
}

/// Weak engine identity whose upgrade retains the complete real strong owner.
///
/// An upgraded recovered handle keeps the native wrapper, its source/tier/root
/// leases and actual cleanup alive for the entire callback or task. Neither an
/// upgrade nor pointer identity establishes liveness or serving permission.
pub struct WeakEngineHandle {
    owner: WeakOwner,
}
impl WeakEngineHandle {
    /// Retains the actual original owner if it has not begun final destruction.
    pub fn upgrade(&self) -> Option<EngineHandle> {
        let owner = match &self.owner {
            WeakOwner::Ordinary(engine) => Owner::Ordinary(engine.upgrade()?),
            #[cfg(unix)]
            WeakOwner::Recovered(engine) => Owner::Recovered(engine.upgrade()?),
        };
        Some(EngineHandle { owner })
    }

    /// Exact weak allocation identity, including after its strong owner is gone.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (&self.owner, &other.owner) {
            (WeakOwner::Ordinary(left), WeakOwner::Ordinary(right)) => Weak::ptr_eq(left, right),
            #[cfg(unix)]
            (WeakOwner::Recovered(left), WeakOwner::Recovered(right)) => Weak::ptr_eq(left, right),
            #[cfg(unix)]
            _ => false,
        }
    }
}
impl Clone for WeakEngineHandle {
    fn clone(&self) -> Self {
        let owner = match &self.owner {
            WeakOwner::Ordinary(engine) => WeakOwner::Ordinary(Weak::clone(engine)),
            #[cfg(unix)]
            WeakOwner::Recovered(engine) => WeakOwner::Recovered(Weak::clone(engine)),
        };
        Self { owner }
    }
}
impl fmt::Debug for WeakEngineHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeakEngineHandle").finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "engine_handle_tests.rs"]
mod tests;

//! Composable server host for shard-stream distributions.
//!
//! The public binary installs no extensions and therefore exposes only the
//! single-node RF1 product. Separately distributed crates can add routes,
//! readiness checks, and bounded background tasks at compile time.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::Router;
use tokio::sync::watch;

pub type ExtensionTask =
    Pin<Box<dyn Future<Output = Result<(), ServerExtensionError>> + Send + 'static>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionReadiness {
    pub name: &'static str,
    pub ready: bool,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerExtensionError {
    message: String,
}

impl ServerExtensionError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ServerExtensionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ServerExtensionError {}

pub trait ServerExtension: Send + Sync + fmt::Debug {
    fn name(&self) -> &'static str;

    fn routes(&self) -> Router {
        Router::new()
    }

    fn readiness(&self) -> ExtensionReadiness {
        ExtensionReadiness {
            name: self.name(),
            ready: true,
            detail: None,
        }
    }

    fn tasks(&self, _shutdown: watch::Receiver<bool>) -> Vec<ExtensionTask> {
        Vec::new()
    }
}

#[derive(Default)]
pub struct ServerBuilder {
    base_routes: Router,
    extensions: Vec<Arc<dyn ServerExtension>>,
}

impl fmt::Debug for ServerBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerBuilder")
            .field("extension_count", &self.extensions.len())
            .finish_non_exhaustive()
    }
}

impl ServerBuilder {
    #[must_use]
    pub fn new(base_routes: Router) -> Self {
        Self {
            base_routes,
            extensions: Vec::new(),
        }
    }

    #[must_use]
    pub fn extension(mut self, extension: Arc<dyn ServerExtension>) -> Self {
        self.extensions.push(extension);
        self
    }

    #[must_use]
    pub fn readiness(&self) -> Vec<ExtensionReadiness> {
        self.extensions
            .iter()
            .map(|extension| extension.readiness())
            .collect()
    }

    pub fn build_routes(&self) -> Router {
        self.extensions
            .iter()
            .fold(self.base_routes.clone(), |router, extension| {
                router.merge(extension.routes())
            })
    }

    #[must_use]
    pub fn tasks(&self, shutdown: watch::Receiver<bool>) -> Vec<ExtensionTask> {
        self.extensions
            .iter()
            .flat_map(|extension| extension.tasks(shutdown.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct ReadyExtension;

    impl ServerExtension for ReadyExtension {
        fn name(&self) -> &'static str {
            "ready"
        }
    }

    #[test]
    fn extensions_are_compile_time_composed() {
        let builder = ServerBuilder::new(Router::new()).extension(Arc::new(ReadyExtension));
        assert_eq!(
            builder.readiness(),
            vec![ExtensionReadiness {
                name: "ready",
                ready: true,
                detail: None,
            }]
        );
        assert!(builder.tasks(watch::channel(false).1).is_empty());
    }
}

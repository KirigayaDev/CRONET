//! Transport-independent CMDB core. Alembic remains the schema owner.
pub mod api;
pub mod auth;
pub mod config;
pub mod db;
pub mod discovery;
pub mod error;
pub mod imports;
pub mod models;
pub mod repository;
pub mod service;
pub mod workflow;

pub use error::{Error, Result};
pub use service::Service;

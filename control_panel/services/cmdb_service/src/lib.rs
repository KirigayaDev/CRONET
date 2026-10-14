//! Transport-independent CMDB core. Alembic remains the schema owner.
pub mod api;
pub mod auth;
pub mod config;
pub mod db;
pub mod error;
pub mod models;
pub mod repository;
pub mod service;

pub use error::{Error, Result};
pub use service::Service;

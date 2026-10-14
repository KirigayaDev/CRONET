//! Кодек SNMP-типов: чистая логика без сети.
//!
//! * `snmp2::Value` -> protobuf `SnmpValue` (без интерпретации смысла);
//! * разбор и проверка OID;
//! * разбор входящих трапов/informs из байтов датаграммы (`decode_trap`) -
//!   именно эта функция принимает данные от недоверенной сети, поэтому она
//!   не должна паниковать ни на каких входах (см. proptest и `fuzz/`).

mod convert;
mod trap;

pub use convert::{oid_arcs, oid_to_string, parse_oid, value_to_proto, OidError};
pub use trap::{build_inform_response, decode_trap, DecodeError, DecodedTrap, MAX_TRAP_VARBINDS};

//! Сгенерированные protobuf-типы: контракты между сервисами netmon и gNMI.
//!
//! Источник правды - каталог `contracts/proto`. Любая правка схемы должна быть
//! обратно совместимой (только новые поля с новыми номерами, ничего не удалять
//! и не переиспользовать номера).

#![allow(clippy::all, clippy::pedantic, clippy::nursery)]

pub mod netmon {
    pub mod v1 {
        tonic::include_proto!("netmon.v1");
    }
}

pub mod gnmi {
    tonic::include_proto!("gnmi");
}

pub mod gnmi_ext {
    tonic::include_proto!("gnmi_ext");
}

/// Имена NATS-субъектов и ресурсов JetStream, общие для всех сервисов.
pub mod subjects {
    /// Задачи на опрос SNMP конкретной площадки: `tasks.snmp.<site>`.
    pub fn snmp_task(site: &str) -> String {
        format!("tasks.snmp.{site}")
    }
    /// Сырой результат опроса: `raw.snmp.<site>`.
    pub fn snmp_result(site: &str) -> String {
        format!("raw.snmp.{site}")
    }
    /// Сырой трап: `raw.trap.<site>`.
    pub fn trap(site: &str) -> String {
        format!("raw.trap.{site}")
    }
    /// Сырое gNMI-обновление: `raw.gnmi.<site>`.
    pub fn gnmi(site: &str) -> String {
        format!("raw.gnmi.{site}")
    }

    /// Normalized metrics, ready for ClickHouse storage.
    pub fn metric(site: &str) -> String {
        format!("metrics.{site}")
    }

    /// Normalized logs, ready for ClickHouse storage.
    pub fn log(site: &str) -> String {
        format!("logs.{site}")
    }

    pub const TASKS_STREAM: &str = "TASKS_SNMP";
    pub const TASKS_FILTER: &str = "tasks.snmp.>";
    pub const RAW_SNMP_STREAM: &str = "RAW_SNMP";
    pub const RAW_SNMP_FILTER: &str = "raw.snmp.>";
    pub const RAW_TRAP_STREAM: &str = "RAW_TRAP";
    pub const RAW_TRAP_FILTER: &str = "raw.trap.>";
    pub const RAW_GNMI_STREAM: &str = "RAW_GNMI";
    pub const RAW_GNMI_FILTER: &str = "raw.gnmi.>";
    pub const METRICS_STREAM: &str = "METRICS";
    pub const METRICS_FILTER: &str = "metrics.>";
    pub const LOGS_STREAM: &str = "LOGS";
    pub const LOGS_FILTER: &str = "logs.>";
    pub const KV_GNMI_DESIRED: &str = "gnmi-desired";
    pub const KV_GNMI_LEASES: &str = "gnmi-leases";
}

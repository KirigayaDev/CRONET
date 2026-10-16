use super::InventorySnapshot;
use crate::{Error, Result};
const HEADERS: [&str; 12] = [
    "index",
    "class",
    "name",
    "description",
    "serial",
    "model",
    "hardware_rev",
    "firmware_rev",
    "software_rev",
    "contained_in",
    "parent_rel_pos",
    "vendor_type",
];

fn rows(snapshot: &InventorySnapshot) -> impl Iterator<Item = Vec<String>> + '_ {
    snapshot.components.iter().map(|c| {
        vec![
            c.ent_physical_index.to_string(),
            c.class.clone(),
            c.name.clone().unwrap_or_default(),
            c.description.clone().unwrap_or_default(),
            c.serial.clone().unwrap_or_default(),
            c.model.clone().unwrap_or_default(),
            c.hardware_rev.clone().unwrap_or_default(),
            c.firmware_rev.clone().unwrap_or_default(),
            c.software_rev.clone().unwrap_or_default(),
            c.contained_in.map(|v| v.to_string()).unwrap_or_default(),
            c.parent_rel_pos.map(|v| v.to_string()).unwrap_or_default(),
            c.vendor_type.clone().unwrap_or_default(),
        ]
    })
}
pub fn export_csv(snapshot: &InventorySnapshot) -> Result<Vec<u8>> {
    let mut writer = csv::Writer::from_writer(Vec::new());
    writer.write_record(HEADERS).map_err(|_| Error::Internal("CSV header failed"))?;
    for row in rows(snapshot) {
        // Spreadsheet applications must not execute formulas from SNMP text.
        let row = row.into_iter().enumerate().map(|(i, v)| {
            if ![0, 9, 10].contains(&i) && v.starts_with(['=', '+', '-', '@', '\t', '\r']) {
                format!("'{v}")
            } else {
                v
            }
        });
        writer.write_record(row).map_err(|_| Error::Internal("CSV row failed"))?;
    }
    writer.into_inner().map_err(|_| Error::Internal("CSV serialization failed"))
}
pub fn export_xlsx(snapshot: &InventorySnapshot) -> Result<Vec<u8>> {
    let mut workbook = rust_xlsxwriter::Workbook::new();
    let sheet = workbook.add_worksheet();
    let failed = |_| Error::Internal("XLSX serialization failed");
    for (col, header) in HEADERS.iter().enumerate() {
        sheet.write_string(0, col as u16, *header).map_err(failed)?;
    }
    for (row, values) in rows(snapshot).enumerate() {
        for (col, value) in values.iter().enumerate() {
            // Explicit strings preserve serial leading zeros and never create formulas.
            sheet.write_string(row as u32 + 1, col as u16, value).map_err(failed)?;
        }
    }
    workbook.save_to_buffer().map_err(failed)
}

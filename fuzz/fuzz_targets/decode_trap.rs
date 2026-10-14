#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = snmp_types::decode_trap(data);
    let _ = snmp_types::build_inform_response(data);
});

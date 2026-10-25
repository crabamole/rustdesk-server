//! Device admission (rustdesk docs/design-device-admission.md).

// Task 6 (enforcement) is the first user of PENDING.
#[allow(dead_code)]
pub(crate) const PENDING: i16 = 0;
pub(crate) const ADMITTED: i16 = 1;

use anyhow::Result;
use serde::Serialize;

/// Print `value` as JSON when `json` is set, otherwise as its human `Display` form.
pub fn emit<T: Serialize + std::fmt::Display>(json: bool, value: &T) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(value)?);
    } else {
        println!("{value}");
    }
    Ok(())
}

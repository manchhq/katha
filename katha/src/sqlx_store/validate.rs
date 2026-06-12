//! Store name validation for SQL safety.

/// Validates that a store name is safe for use in SQL identifiers.
///
/// The name must match `^[a-zA-Z_][a-zA-Z0-9_]*$` to prevent SQL injection
/// when the name is interpolated into table names.
pub fn validate_store_name(name: &str) -> anyhow::Result<()> {
    if name.is_empty() {
        anyhow::bail!("Store name cannot be empty");
    }
    let mut chars = name.chars();
    let first = chars.next().expect("non-empty");
    if !first.is_ascii_alphabetic() && first != '_' {
        anyhow::bail!(
            "Store name must start with a letter or underscore, got: {:?}",
            first
        );
    }
    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' {
            anyhow::bail!(
                "Store name may only contain letters, digits, and underscores, got: {:?}",
                c
            );
        }
    }
    Ok(())
}

//! SQL dialect detection and parameter-placeholder rewriting.
//!
//! The store runs over sqlx's `Any` driver, which passes query strings to the
//! underlying backend verbatim — it does **not** translate bind placeholders.
//! SQLite expects positional `?`; Postgres expects ordinal `$1`, `$2`, … . All
//! queries in this crate are authored with `?` (the SQLite form); [`Backend`]
//! rewrites them for Postgres at execution time.

/// Which SQL dialect the underlying pool speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Backend {
    Sqlite,
    Postgres,
}

impl Backend {
    /// Detects the backend from a database URL scheme.
    ///
    /// Postgres schemes (`postgres://`, `postgresql://`) map to
    /// [`Backend::Postgres`]; everything else (SQLite file/memory URLs) maps to
    /// [`Backend::Sqlite`], which is also the safe default.
    pub(crate) fn from_url(url: &str) -> Self {
        if url.starts_with("postgres:") || url.starts_with("postgresql:") {
            Backend::Postgres
        } else {
            Backend::Sqlite
        }
    }

    /// Rewrites the `?` bind placeholders in `sql` to the dialect's syntax.
    ///
    /// SQLite keeps positional `?`; Postgres needs ordinal `$1`, `$2`, … in the
    /// order the placeholders appear (which matches the order binds are added).
    ///
    /// This is a plain character scan, which is safe here because the only `?`
    /// characters in this crate's queries are bind placeholders: store names are
    /// validated to `[A-Za-z0-9_]` before interpolation, and no SQL literal in
    /// this crate contains a `?`.
    pub(crate) fn bind(self, sql: &str) -> String {
        match self {
            Backend::Sqlite => sql.to_string(),
            Backend::Postgres => {
                let mut out = String::with_capacity(sql.len() + 8);
                let mut idx = 0u32;
                for ch in sql.chars() {
                    if ch == '?' {
                        idx += 1;
                        out.push('$');
                        out.push_str(&idx.to_string());
                    } else {
                        out.push(ch);
                    }
                }
                out
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Backend;

    #[test]
    fn detects_postgres_schemes() {
        assert_eq!(
            Backend::from_url("postgres://u:p@localhost/db"),
            Backend::Postgres
        );
        assert_eq!(
            Backend::from_url("postgresql://u:p@localhost/db"),
            Backend::Postgres
        );
    }

    #[test]
    fn defaults_to_sqlite() {
        assert_eq!(Backend::from_url("sqlite::memory:"), Backend::Sqlite);
        assert_eq!(
            Backend::from_url("sqlite:///tmp/x.db?mode=rwc"),
            Backend::Sqlite
        );
    }

    #[test]
    fn sqlite_keeps_question_marks() {
        let sql = "SELECT * FROM t WHERE a = ? AND b = ?";
        assert_eq!(Backend::Sqlite.bind(sql), sql);
    }

    #[test]
    fn postgres_numbers_placeholders_in_order() {
        assert_eq!(
            Backend::Postgres.bind("INSERT INTO t (a, b, c) VALUES (?, ?, ?)"),
            "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)"
        );
        assert_eq!(
            Backend::Postgres.bind("WHERE (created_utc, id) < (?, ?) LIMIT ?"),
            "WHERE (created_utc, id) < ($1, $2) LIMIT $3"
        );
    }

    #[test]
    fn postgres_leaves_placeholder_free_sql_untouched() {
        let sql =
            r#"INSERT INTO "x_streams" VALUES (a) ON CONFLICT(id) DO UPDATE SET v = excluded.v"#;
        assert_eq!(Backend::Postgres.bind(sql), sql);
    }
}

//! Hand-rolled forward-only migrations recorded in `schema_versions` (and `PRAGMA user_version`).

use crate::{MetaError, MetaResult};
use rusqlite::{Connection, TransactionBehavior, params};

pub const MIGRATIONS: &[(u32, &str, &str)] = &[(1, "initial", include_str!("sql/0001_initial.sql"))];

pub fn migrate(c: &mut Connection) -> MetaResult<u32> {
    apply(c, MIGRATIONS)
}

pub(crate) fn current_version(c: &Connection) -> rusqlite::Result<u32> {
    c.query_row("SELECT COALESCE(MAX(version), 0) FROM schema_versions", [], |r| {
        r.get(0)
    })
}

/// Test hook: migrate with a custom migration list.
#[cfg(any(test, feature = "test-util"))]
pub fn migrate_with(c: &mut Connection, migrations: &[(u32, &str, &str)]) -> MetaResult<u32> {
    apply(c, migrations)
}

/// Applies `migrations` (ascending versions) in one IMMEDIATE txn each; the current version is read inside
/// the txn, so concurrent openers apply each migration once.
fn apply(c: &mut Connection, migrations: &[(u32, &str, &str)]) -> MetaResult<u32> {
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_versions \
         (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at INTEGER NOT NULL)",
    )?;
    let latest = migrations.last().map_or(0, |m| m.0);
    let too_new = |db: u32| MetaError::SchemaTooNew { db, binary: latest };
    // Fast path: an up-to-date database takes no write lock (re-checked inside each txn when applying).
    let pending = if current_version(c)? < latest { migrations } else { &[] };
    for &(version, name, sql) in pending {
        let failed = |e: rusqlite::Error| MetaError::Migration {
            version,
            msg: e.to_string(),
        };
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failed)?;
        let cur = current_version(&tx).map_err(failed)?;
        if cur > latest {
            return Err(too_new(cur));
        }
        if version <= cur {
            continue; // rolls back the empty txn on drop
        }
        tx.execute_batch(sql).map_err(failed)?;
        tx.execute(
            "INSERT INTO schema_versions (version, name, applied_at) VALUES (?1, ?2, ?3)",
            params![version, name, forge_model::time::now_ms()],
        )
        .map_err(failed)?;
        tx.pragma_update(None, "user_version", version).map_err(failed)?;
        tx.commit().map_err(failed)?;
    }
    let cur = current_version(c)?;
    if cur > latest {
        return Err(too_new(cur));
    }
    Ok(cur)
}

use bytesize::ByteSize;
use figment::Jail;
use forge_model::config::{ForgeConfig, WalMode};
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

const EXAMPLE: &str = include_str!("../../../forge.example.toml");

/// A figment Jail with every inherited env var cleared first.
fn jail(f: impl FnOnce(&mut Jail) -> figment::error::Result<()>) {
    Jail::expect_with(|j| {
        j.clear_env();
        f(j)
    });
}

#[test]
fn empty_jail_is_default() {
    jail(|_| {
        let c = ForgeConfig::load(None)?;
        assert_eq!(c, ForgeConfig::default());
        assert_eq!(c.storage.url, "file://./forge-data/data");
        assert_eq!(c.metastore.url, "sqlite://./forge-data/meta.db");
        assert_eq!(c.wal.dir, Path::new("./forge-data/wal"));
        assert_eq!(
            c,
            ForgeConfig::rooted_at(Path::new("./forge-data")),
            "the default layout"
        );
        assert_eq!(c.server.shutdown_grace, Duration::from_secs(10));
        Ok(())
    });
}

#[test]
fn example_file_loads_with_spec_values() {
    jail(|jail| {
        jail.create_file("example.toml", EXAMPLE)?;
        let c = ForgeConfig::load(Some(Path::new("example.toml")))?;
        assert_eq!(c.storage.url, "file:///var/lib/forge/data");
        assert_eq!(c.metastore.url, "sqlite:///var/lib/forge/meta.db");
        assert_eq!(c.wal.mode, WalMode::Local);
        assert_eq!(c.wal.dir, Path::new("/var/lib/forge/wal"));
        assert_eq!(c.memtable.flush_bytes, ByteSize::mib(64));
        assert_eq!(c.memtable.flush_interval, Duration::from_secs(30));
        assert_eq!(c.memtable.memory_budget, ByteSize::gib(1));
        assert_eq!(c.cache.memory, ByteSize::mib(512));
        assert_eq!(c.cache.disk, ByteSize::gib(10));
        assert_eq!(c.cache.disk_dir, Path::new("/var/lib/forge/cache"));
        assert_eq!(c.compaction.target_segment, ByteSize::mib(256));
        assert_eq!(c.compaction.concurrency, 2);
        // The example's [compaction] keys are the defaults.
        assert_eq!(c.compaction, forge_model::config::CompactionConfig::default());
        assert_eq!(c.compaction.interval, Duration::from_secs(10));
        assert_eq!(c.compaction.l0_trigger, 4);
        assert_eq!(c.compaction.l0_max_age, Duration::from_secs(600));
        assert_eq!(c.compaction.max_job_bytes, ByteSize::mib(512));
        assert_eq!(c.compaction.slice_bytes, ByteSize::mib(128));
        assert_eq!(c.compaction.gc_interval, Duration::from_secs(600));
        assert_eq!(c.compaction.gc_grace, Duration::from_secs(3600));
        assert_eq!(c.compaction.orphan_grace, Duration::from_secs(86_400));
        assert_eq!(c.query.memory_limit, ByteSize::gib(2));
        assert_eq!(c.query.timeout, Duration::from_secs(30));
        Ok(())
    });
}

#[test]
fn env_overrides_file() {
    jail(|jail| {
        jail.create_file("forge.toml", EXAMPLE)?;
        jail.set_env("FORGE_STORAGE__URL", "memory://");
        jail.set_env("FORGE_MEMTABLE__FLUSH_BYTES", "128MiB");
        jail.set_env("FORGE_QUERY__TIMEOUT", "1m30s");
        jail.set_env("FORGE_WAL__MODE", "object_store");
        jail.set_env("FORGE_SERVER__ADMIN_ADDR", "127.0.0.1:0");
        jail.set_env("FORGE_COMPACTION__TARGET_SEGMENT", "512MiB");
        jail.set_env("FORGE_SERVER__SHUTDOWN_GRACE", "1s");
        jail.set_env("FORGE_COMPACTION__INTERVAL", "0s");
        jail.set_env("FORGE_COMPACTION__GC_GRACE", "200ms");
        let c = ForgeConfig::load(None)?;
        assert_eq!(c.compaction.interval, Duration::ZERO);
        assert_eq!(c.compaction.gc_grace, Duration::from_millis(200));
        assert_eq!(c.storage.url, "memory://");
        assert_eq!(c.memtable.flush_bytes, ByteSize::mib(128));
        assert_eq!(c.query.timeout, Duration::from_secs(90));
        assert_eq!(c.wal.mode, WalMode::ObjectStore);
        assert_eq!(c.server.admin_addr, "127.0.0.1:0".parse::<SocketAddr>().unwrap());
        assert_eq!(c.compaction.target_segment, ByteSize::mib(512));
        assert_eq!(c.server.shutdown_grace, Duration::from_secs(1));
        // Untouched siblings keep the file's values.
        assert_eq!(c.memtable.flush_interval, Duration::from_secs(30));
        assert_eq!(c.memtable.memory_budget, ByteSize::gib(1));
        assert_eq!(c.metastore.url, "sqlite:///var/lib/forge/meta.db");
        Ok(())
    });
}

#[test]
fn cli_env_vars_are_ignored() {
    jail(|jail| {
        jail.set_env("FORGE_CONFIG", "x");
        jail.set_env("FORGE_LOG", "debug");
        jail.set_env("FORGE_LOG_FORMAT", "json");
        assert_eq!(ForgeConfig::load(None)?, ForgeConfig::default());
        Ok(())
    });
}

#[test]
fn string_fields_accept_scalars() {
    jail(|jail| {
        jail.set_env("FORGE_NODE__ID", "123");
        assert_eq!(ForgeConfig::load(None)?.node.id, "123");
        jail.set_env("FORGE_NODE__ID", "true");
        assert_eq!(ForgeConfig::load(None)?.node.id, "true");
        Ok(())
    });
    jail(|jail| {
        jail.create_file("forge.toml", "[node]\nid = 7\n")?;
        assert_eq!(ForgeConfig::load(None)?.node.id, "7");
        Ok(())
    });
}

#[test]
fn string_and_path_env_values_keep_their_text() {
    for raw in ["007", "1.0", "+5", "-0", "1e3", "true", "123"] {
        jail(|jail| {
            jail.set_env("FORGE_STORAGE__URL", raw);
            jail.set_env("FORGE_METASTORE__URL", raw);
            jail.set_env("FORGE_WAL__DIR", raw);
            jail.set_env("FORGE_CACHE__DISK_DIR", raw);
            let c = ForgeConfig::load(None)?;
            assert_eq!(c.storage.url, raw);
            assert_eq!(c.metastore.url, raw);
            assert_eq!(c.wal.dir, Path::new(raw));
            assert_eq!(c.cache.disk_dir, Path::new(raw));
            Ok(())
        });
    }
    // node.id must also be a valid project id, so it gets only valid ids.
    for raw in ["007", "1.0", "-0", "1e3", "true", "123"] {
        jail(|jail| {
            jail.set_env("FORGE_NODE__ID", raw);
            jail.set_env("FORGE_INGEST__DEFAULT_PROJECT", raw);
            let c = ForgeConfig::load(None)?;
            assert_eq!(c.node.id, raw);
            assert_eq!(c.ingest.default_project, raw);
            Ok(())
        });
    }
    // Non-string keys are still typed from env.
    jail(|jail| {
        jail.set_env("FORGE_COMPACTION__CONCURRENCY", "007");
        assert_eq!(ForgeConfig::load(None)?.compaction.concurrency, 7);
        Ok(())
    });
}

#[test]
fn empty_node_id_is_rejected() {
    jail(|jail| {
        jail.set_env("FORGE_NODE__ID", "");
        let e = ForgeConfig::load(None).unwrap_err().to_string();
        assert!(e.contains("node.id: invalid project id"), "{e}");
        Ok(())
    });
    jail(|jail| {
        jail.create_file("forge.toml", "[node]\nid = \"  \"\n")?;
        let e = ForgeConfig::load(None).unwrap_err().to_string();
        assert!(e.contains("node.id: invalid project id"), "{e}");
        Ok(())
    });
}

#[test]
fn unknown_and_malformed_values_error() {
    jail(|jail| {
        jail.create_file("forge.toml", "[cache]\nmemroy = \"1GiB\"\n")?;
        let e = ForgeConfig::load(None).unwrap_err().to_string();
        assert!(e.contains("memroy"), "{e}");
        Ok(())
    });
    jail(|jail| {
        jail.set_env("FORGE_MEMTABLE__FLUSH_BYTEZ", "1");
        assert!(ForgeConfig::load(None).is_err());
        Ok(())
    });
    jail(|jail| {
        jail.set_env("FORGE_MEMTABLE__FLUSH_BYTES", "lots");
        assert!(ForgeConfig::load(None).is_err());
        Ok(())
    });
}

#[test]
fn explicit_missing_file_errors() {
    jail(|_| {
        assert!(ForgeConfig::load(Some(Path::new("missing.toml"))).is_err());
        Ok(())
    });
}

#[test]
fn implicit_forge_toml_is_read_from_cwd_only() {
    jail(|jail| {
        jail.create_file("forge.toml", "[node]\nid = \"here\"\n")?;
        assert_eq!(ForgeConfig::load(None)?.node.id, "here");
        std::fs::create_dir("sub").expect("mkdir sub");
        jail.change_dir("sub")?;
        assert_eq!(ForgeConfig::load(None)?.node.id, "node-1");
        Ok(())
    });
}

#[test]
fn m1_keys() {
    use forge_model::config::IngestConfig;
    jail(|jail| {
        let c = ForgeConfig::load(None)?;
        assert_eq!(c.ingest, IngestConfig::default());
        assert_eq!(c.ingest.default_project, "default");
        assert_eq!(c.wal.segment_bytes, ByteSize::mib(64));
        jail.create_file("example.toml", EXAMPLE)?;
        let c = ForgeConfig::load(Some(Path::new("example.toml")))?;
        assert_eq!(c.ingest.blob_threshold, ByteSize::mib(1));
        assert_eq!(c.wal.group_commit_max_delay, Duration::from_millis(10));
        jail.set_env("FORGE_INGEST__DEFAULT_PROJECT", "007");
        jail.set_env("FORGE_WAL__GROUP_COMMIT_MAX_DELAY", "5ms");
        let c = ForgeConfig::load(None)?;
        assert_eq!(c.ingest.default_project, "007");
        assert_eq!(c.wal.group_commit_max_delay, Duration::from_millis(5));
        Ok(())
    });
    jail(|jail| {
        jail.set_env("FORGE_NODE__ID", "a/b");
        assert!(ForgeConfig::load(None).is_err());
        Ok(())
    });
    jail(|jail| {
        jail.set_env("FORGE_MEMTABLE__FLUSH_BYTES", "2GiB");
        assert!(ForgeConfig::load(None).is_err());
        Ok(())
    });
}

/// Review 1 #11 / review 2 R2-1: `orphan_grace` below 4 x flush_interval + 10 min is rejected at load.
#[test]
fn orphan_grace_is_validated() {
    jail(|jail| {
        for bad in ["0s", "5m", "11m59s"] {
            jail.set_env("FORGE_COMPACTION__ORPHAN_GRACE", bad);
            let e = ForgeConfig::load(None).unwrap_err().to_string();
            assert!(e.contains("orphan_grace") && e.contains("flush_interval"), "{bad}: {e}");
        }
        jail.set_env("FORGE_COMPACTION__ORPHAN_GRACE", "12m"); // 4 x 30 s + 10 min
        assert_eq!(
            ForgeConfig::load(None)?.compaction.orphan_grace,
            Duration::from_secs(720)
        );
        jail.set_env("FORGE_MEMTABLE__FLUSH_INTERVAL", "10m");
        assert!(ForgeConfig::load(None).is_err(), "12m < 4 x 10m + 10m");
        jail.set_env("FORGE_COMPACTION__ORPHAN_GRACE", "50m");
        assert!(ForgeConfig::load(None).is_ok());
        Ok(())
    });
    let c = forge_model::config::CompactionConfig::default();
    assert_eq!(c.blob_refresh(), Duration::from_secs(3600), "min(1 h, 24 h / 4)");
    let c = forge_model::config::CompactionConfig {
        orphan_grace: Duration::from_secs(3600),
        ..c
    };
    assert_eq!(c.blob_refresh(), Duration::from_secs(900));
}

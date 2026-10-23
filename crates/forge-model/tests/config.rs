use bytesize::ByteSize;
use figment::Jail;
use forge_model::config::{ForgeConfig, WalMode};
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

const EXAMPLE: &str = include_str!("../../../forge.example.toml");

#[test]
fn empty_jail_is_default() {
    Jail::expect_with(|jail| {
        jail.clear_env();
        let c = ForgeConfig::load(None)?;
        assert_eq!(c, ForgeConfig::default());
        assert_eq!(c.storage.url, "file://./forge-data/data");
        assert_eq!(c.metastore.url, "sqlite://./forge-data/meta.db");
        assert_eq!(c.server.shutdown_grace, Duration::from_secs(10));
        Ok(())
    });
}

#[test]
fn example_file_loads_with_spec_values() {
    Jail::expect_with(|jail| {
        jail.clear_env();
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
        assert_eq!(c.query.memory_limit, ByteSize::gib(2));
        assert_eq!(c.query.timeout, Duration::from_secs(30));
        Ok(())
    });
}

#[test]
fn env_overrides_file() {
    Jail::expect_with(|jail| {
        jail.clear_env();
        jail.create_file("forge.toml", EXAMPLE)?;
        jail.set_env("FORGE_STORAGE__URL", "memory://");
        jail.set_env("FORGE_MEMTABLE__FLUSH_BYTES", "128MiB");
        jail.set_env("FORGE_QUERY__TIMEOUT", "1m30s");
        jail.set_env("FORGE_WAL__MODE", "object_store");
        jail.set_env("FORGE_SERVER__ADMIN_ADDR", "127.0.0.1:0");
        jail.set_env("FORGE_COMPACTION__TARGET_SEGMENT", "512MiB");
        jail.set_env("FORGE_SERVER__SHUTDOWN_GRACE", "1s");
        let c = ForgeConfig::load(None)?;
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
    Jail::expect_with(|jail| {
        jail.clear_env();
        jail.set_env("FORGE_CONFIG", "x");
        jail.set_env("FORGE_LOG", "debug");
        jail.set_env("FORGE_LOG_FORMAT", "json");
        assert_eq!(ForgeConfig::load(None)?, ForgeConfig::default());
        Ok(())
    });
}

#[test]
fn string_fields_accept_scalars() {
    Jail::expect_with(|jail| {
        jail.clear_env();
        jail.set_env("FORGE_NODE__ID", "123");
        assert_eq!(ForgeConfig::load(None)?.node.id, "123");
        jail.set_env("FORGE_NODE__ID", "true");
        assert_eq!(ForgeConfig::load(None)?.node.id, "true");
        Ok(())
    });
    Jail::expect_with(|jail| {
        jail.clear_env();
        jail.create_file("forge.toml", "[node]\nid = 7\n")?;
        assert_eq!(ForgeConfig::load(None)?.node.id, "7");
        Ok(())
    });
}

#[test]
fn unknown_and_malformed_values_error() {
    Jail::expect_with(|jail| {
        jail.clear_env();
        jail.create_file("forge.toml", "[cache]\nmemroy = \"1GiB\"\n")?;
        let e = ForgeConfig::load(None).unwrap_err().to_string();
        assert!(e.contains("memroy"), "{e}");
        Ok(())
    });
    Jail::expect_with(|jail| {
        jail.clear_env();
        jail.set_env("FORGE_MEMTABLE__FLUSH_BYTEZ", "1");
        assert!(ForgeConfig::load(None).is_err());
        Ok(())
    });
    Jail::expect_with(|jail| {
        jail.clear_env();
        jail.set_env("FORGE_MEMTABLE__FLUSH_BYTES", "lots");
        assert!(ForgeConfig::load(None).is_err());
        Ok(())
    });
}

#[test]
fn explicit_missing_file_errors() {
    Jail::expect_with(|jail| {
        jail.clear_env();
        assert!(ForgeConfig::load(Some(Path::new("missing.toml"))).is_err());
        Ok(())
    });
}

#[test]
fn implicit_forge_toml_is_read_from_cwd_only() {
    Jail::expect_with(|jail| {
        jail.clear_env();
        jail.create_file("forge.toml", "[node]\nid = \"here\"\n")?;
        assert_eq!(ForgeConfig::load(None)?.node.id, "here");
        std::fs::create_dir("sub").expect("mkdir sub");
        jail.change_dir("sub")?;
        assert_eq!(ForgeConfig::load(None)?.node.id, "node-1");
        Ok(())
    });
}

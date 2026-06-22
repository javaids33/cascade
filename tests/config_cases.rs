//! Config-contract tests — the guard that build changes don't break how users pass configs.
//! Pure: no tursodb, no Ollama, no network. Runs in plain `cargo test`.

use cascade::config::Config;
use cascade::source::Source;
use cascade::ollama::vec_to_json;
use cascade::Role;

#[test]
fn shipped_master_config_parses() {
    let c = Config::from_path("configs/master.toml").expect("configs/master.toml must parse");
    assert_eq!(c.node.role, Role::Master);
    assert!(c.is_master());
    assert!(c.sync.enabled && c.sync.serve, "master should serve the hub");
    assert_eq!(c.embedding.dim, 384);
    assert_eq!(c.source.kind, "wikimedia");
    assert!(c.olap.duckdb.is_some(), "master has an OLAP target");
}

#[test]
fn shipped_replica_config_parses() {
    let c = Config::from_path("configs/replica.toml").expect("configs/replica.toml must parse");
    assert_eq!(c.node.role, Role::Replica);
    assert!(!c.is_master());
    assert!(c.sync.enabled);
    assert!(c.sync.remote_url.contains("MASTER_IP"), "replica points at the master");
}

#[test]
fn shipped_local_smoke_configs_parse() {
    let m = Config::from_path("configs/local-master.toml").expect("local-master parses");
    assert_eq!(m.node.role, Role::Master);
    assert!(!m.sync.serve, "smoke master does not own the hub");
    assert_eq!(m.source.kind, "demo");
    let r = Config::from_path("configs/local-replica.toml").expect("local-replica parses");
    assert_eq!(r.node.role, Role::Replica);
    assert!(r.generation.model.is_empty(), "smoke replica is retrieval-only");
}

#[test]
fn embedded_examples_parse() {
    let m: Config = toml::from_str(Config::example_master()).expect("example_master parses");
    assert_eq!(m.node.role, Role::Master);
    let r: Config = toml::from_str(Config::example_replica()).expect("example_replica parses");
    assert_eq!(r.node.role, Role::Replica);
}

#[test]
fn defaults_apply_for_minimal_config() {
    // A user supplies only the required [node] section; everything else should default sanely.
    let c: Config = toml::from_str("[node]\nrole = \"master\"\ndb = \"x.db\"\n").unwrap();
    assert!(!c.sync.enabled, "sync defaults off (standalone local node)");
    assert_eq!(c.sync.remote_url, "http://127.0.0.1:8080");
    assert_eq!(c.sync.push_every, 32);
    assert!(c.cdc.enabled, "cdc defaults on");
    assert_eq!(c.embedding.model, "all-minilm");
    assert_eq!(c.embedding.dim, 384);
    assert_eq!(c.source.kind, "none");
    assert!(c.olap.duckdb.is_none());
}

#[test]
fn invalid_role_is_rejected() {
    let bad = toml::from_str::<Config>("[node]\nrole = \"primary\"\ndb = \"x.db\"\n");
    assert!(bad.is_err(), "unknown role must fail to parse, not silently default");
}

#[test]
fn missing_required_node_is_rejected() {
    assert!(toml::from_str::<Config>("[sync]\nenabled = true\n").is_err());
}

#[test]
fn source_kinds_map_correctly() {
    assert_eq!(Source::parse("wikimedia"), Source::Wikimedia);
    assert_eq!(Source::parse("hn"), Source::Hn);
    assert_eq!(Source::parse("demo"), Source::Demo);
    assert_eq!(Source::parse("anything-else"), Source::None);
}

#[test]
fn vector_json_format_is_turso_compatible() {
    assert_eq!(vec_to_json(&[]), "[]");
    assert_eq!(vec_to_json(&[1.0, -0.5]), "[1.000000,-0.500000]");
}

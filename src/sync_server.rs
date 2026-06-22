//! Spawn and supervise the `tursodb --sync-server` hub (the neutral primary that masters push to
//! and replicas pull from). Killed on drop.

use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

pub struct SyncServer {
    child: Child,
    pub bind: String,
}

impl SyncServer {
    /// Find the tursodb CLI: `$TURSODB`, else `<dir>/**/tursodb` under common locations, else PATH.
    pub fn find_tursodb(search_dirs: &[PathBuf]) -> Option<PathBuf> {
        if let Ok(p) = std::env::var("TURSODB") {
            let pb = PathBuf::from(p);
            if pb.is_file() {
                return Some(pb);
            }
        }
        fn walk(dir: &std::path::Path) -> Option<PathBuf> {
            let rd = std::fs::read_dir(dir).ok()?;
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    if let Some(f) = walk(&p) {
                        return Some(f);
                    }
                } else if p.file_name().map(|n| n == "tursodb").unwrap_or(false) {
                    return Some(p);
                }
            }
            None
        }
        for d in search_dirs {
            if let Some(f) = walk(d) {
                return Some(f);
            }
        }
        which_tursodb()
    }

    /// Start `tursodb <db_path> --sync-server <bind>` and wait until it accepts connections.
    pub fn start(bind: &str, db_path: &str, search_dirs: &[PathBuf]) -> Result<Self> {
        let tursodb = Self::find_tursodb(search_dirs)
            .context("tursodb CLI not found (run ./setup.sh or set TURSODB)")?;
        if let Some(p) = std::path::Path::new(db_path).parent() {
            let _ = std::fs::create_dir_all(p);
        }
        let child = Command::new(&tursodb)
            .arg(db_path)
            .arg("--sync-server")
            .arg(bind)
            .spawn()
            .with_context(|| format!("spawn {}", tursodb.display()))?;

        let port: u16 = bind.rsplit(':').next().and_then(|s| s.parse().ok()).unwrap_or(8080);
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return Ok(SyncServer { child, bind: bind.to_string() });
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        let mut s = SyncServer { child, bind: bind.to_string() };
        let _ = s.child.kill();
        anyhow::bail!("sync server failed to start on {bind}");
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }
}

impl Drop for SyncServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn which_tursodb() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|d| d.join("tursodb"))
        .find(|p| p.is_file())
}

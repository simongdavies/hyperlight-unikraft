//! VM density demo: load a golden snapshot **once**, then create many sandboxes
//! that share its memory copy-on-write.
//!
//! Usage:
//!   cargo run --example density -- <snapshot_dir> <initrd.cpio> [code] [count]
//!
//! Each sandbox is built from the same `Arc<Snapshot>` via [`Sandbox::from_snapshot`],
//! so Hyperlight maps the golden read-only/shared and only the pages a VM dirties cost
//! per-VM memory — N sandboxes is far cheaper than N independent full VMs. The shared
//! handle's strong count rises as each sandbox takes a reference to the same golden.

use std::path::PathBuf;
use std::sync::Arc;

use hyperlight_unikraft::Sandbox;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: density <snapshot_dir> <initrd.cpio> [code] [count]";
    let snapshot: PathBuf = args.next().ok_or_else(|| anyhow::anyhow!(usage))?.into();
    let initrd: PathBuf = args.next().ok_or_else(|| anyhow::anyhow!(usage))?.into();
    let code = args
        .next()
        .unwrap_or_else(|| "import sys; print('density says hi'); sys.exit(0)".to_string());
    let count: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(2);

    // Load the golden ONCE — this is the per-node cost paid a single time.
    let t = std::time::Instant::now();
    let golden = Sandbox::load_snapshot(&snapshot)?;
    eprintln!(
        "[density] loaded golden in {}ms; building {count} sandboxes from the shared Arc",
        t.elapsed().as_millis()
    );

    // Build many sandboxes from the SAME Arc. The strong count rising proves they all
    // reference the one golden rather than re-loading it per VM.
    let mut sandboxes = Vec::with_capacity(count);
    for i in 0..count {
        let t = std::time::Instant::now();
        let sb = Sandbox::from_snapshot(golden.clone(), &[], Some(initrd.clone()), None, None)?;
        eprintln!(
            "[density] sandbox {i} built in {}ms (golden Arc strong_count = {})",
            t.elapsed().as_millis(),
            Arc::strong_count(&golden)
        );
        sandboxes.push(sb);
    }

    // Run the code in each, independently — every sandbox rewinds to the shared golden.
    for (i, sb) in sandboxes.iter_mut().enumerate() {
        let t = std::time::Instant::now();
        let out = sb.run_code(&code)?;
        eprintln!(
            "[density] sandbox {i} run={}ms exit={}",
            t.elapsed().as_millis(),
            out.exit_code
        );
        print!("{}", out.stdout);
        eprint!("{}", out.stderr);
    }

    eprintln!("[density] all {count} sandboxes ran from one shared golden snapshot");
    Ok(())
}

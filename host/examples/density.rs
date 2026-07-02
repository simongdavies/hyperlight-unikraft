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
    let initrd_arg = args.next().ok_or_else(|| anyhow::anyhow!(usage))?;
    // "none" = the initrd is baked into the snapshot (inline bake); don't re-map it.
    let initrd: Option<PathBuf> = if initrd_arg == "none" {
        None
    } else {
        Some(initrd_arg.into())
    };
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

    // Measure destroy+recreate per exec (set HL_RECREATE): create a fresh VM from the
    // shared golden, run once, then drop it (destroying the partition), repeated. This is
    // the cross-trust "new VM per exec" cost, vs the reuse-via-restore hot path.
    if std::env::var_os("HL_RECREATE").is_some() {
        for i in 0..count {
            let t = std::time::Instant::now();
            let mut sb = Sandbox::from_snapshot(golden.clone(), &[], initrd.clone(), None, None)?;
            let create_ms = t.elapsed().as_millis();
            let t2 = std::time::Instant::now();
            let out = sb.run_code(&code)?;
            let run_ms = t2.elapsed().as_millis();
            drop(sb); // destroy the partition (WHvDeletePartition)
            eprintln!(
                "[recreate] iter {i}: create={create_ms}ms run={run_ms}ms total={}ms exit={}",
                create_ms + run_ms,
                out.exit_code
            );
            print!("{}", out.stdout);
        }
        return Ok(());
    }

    // Build many sandboxes from the SAME Arc. The strong count rising proves they all
    // reference the one golden rather than re-loading it per VM.
    let mut sandboxes = Vec::with_capacity(count);
    for i in 0..count {
        let t = std::time::Instant::now();
        let sb = Sandbox::from_snapshot(golden.clone(), &[], initrd.clone(), None, None)?;
        eprintln!(
            "[density] sandbox {i} built in {}ms (golden Arc strong_count = {})",
            t.elapsed().as_millis(),
            Arc::strong_count(&golden)
        );
        sandboxes.push(sb);
    }

    // Run the code in each sandbox several times. The FIRST restore per sandbox may pay a
    // one-off cost (re-mapping the golden into the VM partition); repeats should reveal the
    // true warm per-exec restore. Set HL_RESTORE_TIMING to see the inner.restore vs initrd split.
    for (i, sb) in sandboxes.iter_mut().enumerate() {
        for r in 0..3 {
            let t = std::time::Instant::now();
            let out = sb.run_code(&code)?;
            eprintln!(
                "[density] sandbox {i} run {r} = {}ms exit={}",
                t.elapsed().as_millis(),
                out.exit_code
            );
            print!("{}", out.stdout);
            eprint!("{}", out.stderr);
        }
    }

    eprintln!("[density] all {count} sandboxes ran from one shared golden snapshot");
    Ok(())
}

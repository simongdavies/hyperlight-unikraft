//! Bake a warm golden snapshot from a kernel + initrd, mirroring `pyhl setup`.
//!
//! Boots the python-agent-driver, pays the one-time Python warmup (Py_Initialize
//! + preloaded imports during init), exercises the run path once so any
//! first-call lazy init is baked in, then persists the warm post-init snapshot
//! to an OCI directory. `from_snapshot` / the `density` example can then restore
//! it per-exec without paying boot or warmup again.
//!
//! Usage:
//!   cargo run --example bake -- <kernel> <initrd.cpio> <out_snapshot_dir> [heap_mib]

use std::path::PathBuf;
use std::time::Instant;

use hyperlight_unikraft::Sandbox;

/// Default guest heap, matching `pyhl setup` so the golden is directly
/// comparable with pyhl-produced snapshots.
const DEFAULT_HEAP_MIB: u64 = 1280;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: bake <kernel> <initrd.cpio> <out_snapshot_dir> [heap_mib] [inline]";
    let kernel: PathBuf = args.next().ok_or_else(|| anyhow::anyhow!(usage))?.into();
    let initrd: PathBuf = args.next().ok_or_else(|| anyhow::anyhow!(usage))?.into();
    let out: PathBuf = args.next().ok_or_else(|| anyhow::anyhow!(usage))?.into();
    let heap_mib: u64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_HEAP_MIB);
    // `inline` bakes the initrd INTO the snapshot (copied into guest memory) so
    // restore never re-maps it; the default `file` mode maps it zero-copy and
    // re-maps it on every restore (~28ms/restore for a 1.13 GB initrd).
    let inline = args.next().map(|s| s == "inline").unwrap_or(false);

    let t = Instant::now();
    let builder = Sandbox::builder(&kernel).heap_size(heap_mib * 1024 * 1024);
    let builder = if inline {
        builder.initrd_bytes(std::fs::read(&initrd)?)
    } else {
        builder.initrd_file(&initrd)
    };
    let mut sbox = builder.build()?;
    eprintln!(
        "[bake] booted + warmed in {:.1}s ({} initrd); baking first-run cost…",
        t.elapsed().as_secs_f64(),
        if inline { "inline" } else { "mapped" }
    );

    // Exercise the run path once (like `pyhl setup`) so any first-call lazy init
    // lands in the golden, then capture the warm state and persist it.
    let _ = sbox.run_code("pass")?;
    sbox.snapshot_now()?;
    sbox.save_snapshot(&out)?;

    eprintln!(
        "[bake] warm golden saved to {} (heap {} MiB) in {:.1}s total",
        out.display(),
        heap_mib,
        t.elapsed().as_secs_f64()
    );
    Ok(())
}

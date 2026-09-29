# Kernel

The embedded kernel binary (`elfloader_hyperlight-x86_64`) is a Unikraft app-elfloader built against `unikraft/unikraft`'s `plat-hyperlight-v2` branch, whose `plat/hyperlight` carries the cooperative step model (`step.c`, `/dev/hlcall`) that lets the host drive long-running guests and snapshot them mid-run.

Users don't need to build this — it's embedded in the `hluk` binary via `include_bytes!`. Only the rootfs (built with `just build-rootfs`) needs to be produced by users.

## Configuration

See `../defconfig-elfloader` for the general CPIO/VFS kernel configuration.
`../defconfig-workerd` builds the workerd-specific kernel, which loads a
trusted static PIE directly from the host-mapped initrd and mounts an empty
RAMFS root for `/dev`. This avoids retaining a second full executor copy in
RAMFS before ELF loading.

## Rebuilding from source

The kernel is built from three git submodules pinned under `kernel/`:

| Submodule | Source | Branch |
|-----------|--------|--------|
| `unikraft` | [unikraft/unikraft](https://github.com/unikraft/unikraft) | `plat-hyperlight-v2` |
| `app-elfloader` | [unikraft/app-elfloader](https://github.com/unikraft/app-elfloader) | `staging` |
| `libs/libelf` | [unikraft/lib-libelf](https://github.com/unikraft/lib-libelf) | `staging` |

```bash
# Initialise submodules (first time only)
git submodule update --init --recursive

# Build the kernel
just build-kernel

# Verify a committed binary matches source (CI uses this)
just verify-kernel

# Build and verify the direct-initrd workerd kernel
just build-workerd-kernel
just verify-workerd-kernel
```

The workerd recipe exports each pinned submodule with `git archive` before
building. This keeps the build reproducible from Windows/WSL checkouts where
Git may otherwise materialize shell scripts with CRLF line endings.

The build is reproducible — `CONFIG_LIBUKLIBID_INFO_COMPILEDATE=n` in the defconfig ensures the same source always produces the same binary.

<!-- TODO: upstream kernel changes to kraft so this can be built with
     `kraft build --plat hyperlight --arch x86_64` without manual patching. -->

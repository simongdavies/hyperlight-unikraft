#!/usr/bin/python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.
"""Execute setup and its container commands with bounded, local external stubs."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
EXECUTOR = "workerd-sandbox-executor"
IMAGE = "workerd-hyperlight-builder"
COMMIT = "c13c119d07047350ddba6596eff3a6c072a10a32"


def stub():
    root = Path(os.environ["SETUP_STUB_ROOT"])
    command, args = Path(sys.argv[0]).name, sys.argv[1:]
    with (root / "trace.jsonl").open("a") as trace:
        trace.write(json.dumps([command, *args]) + "\n")
    if command == "id":
        if args == ["-u"]:
            print("1000")
        elif args == ["-g"]:
            print("1000")
        elif args == ["-un"]:
            print("fixture-user")
        elif args == ["-nG"]:
            print(os.environ.get("SETUP_SESSION_GROUPS", "users docker"))
        elif args == ["-nG", "fixture-user"]:
            print(os.environ.get("SETUP_ACCOUNT_GROUPS", "users docker"))
        else:
            raise AssertionError(args)
    elif command == "git":
        if "rev-parse" in args:
            print(COMMIT)
        elif not any(value in args for value in
                     ("merge-base", "diff", "status", "submodule", "fetch", "checkout")):
            raise AssertionError(args)
    elif command == "docker":
        if args == ["info"]:
            error = os.environ.get("SETUP_DOCKER_ERROR")
            if error:
                print(error, file=sys.stderr)
                return 1
        elif args[:2] == ["context", "inspect"]:
            print("unix:///var/run/docker.sock")
        elif args[0] == "build":
            assert args[args.index("--tag") + 1] == IMAGE
            (root / "builder-ready").touch()
        elif args[:2] == ["image", "inspect"]:
            assert args[-1] == IMAGE, args
            assert (root / "builder-ready").exists()
            print("sha256:" + "a" * 64)
        elif args[0] == "run":
            assert args[args.index("--workdir") + 2] == IMAGE, args
            env = os.environ.copy()
            for index, value in enumerate(args):
                if value == "--env":
                    key, value = args[index + 1].split("=", 1)
                    env[key] = value
            env["HOME"] = str(root / "builder-home")
            script = args[-1]
            for old, new in [
                ("/usr/lib/llvm-22/bin/", str(root / "stub-bin") + "/"),
                ("/tmp/libcxx-check", str(root / "libcxx-check")),
                ("/output/", str(root / "build-elfloader/workerd-executor") + "/"),
            ]:
                script = script.replace(old, new)
            cwd = root / "workerd" if args[args.index("--workdir") + 1] == "/workspace" else root
            result = subprocess.run(
                ["/bin/bash", "-c", script], cwd=cwd, env=env,
                timeout=10, check=False,
            )
            return result.returncode
        else:
            raise AssertionError(args)
    elif command == "bazel":
        assert "//src/workerd/server:" + EXECUTOR in args, args
        output = root / "workerd/bazel-bin/src/workerd/server" / EXECUTOR
        if os.environ.get("SETUP_MISSING_EXECUTOR"):
            return 0
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            '#!/bin/sh\nprintf "%s\\n" "$0 $*" >>"$SETUP_STUB_ROOT/self-tests"\n'
            'test "$1" = --self-test\n'
        )
        output.chmod(0o755)
    elif command == "clang++":
        if args == ["--version"]:
            print("clang version 22.1.0")
        else:
            output = Path(args[args.index("-o") + 1])
            output.write_text("#!/bin/sh\nexit 0\n")
            output.chmod(0o755)
    elif command == "dpkg-query":
        print("libc++-22-dev fixture")
    elif command == "bash":
        assert args[0] == "examples/workerd-executor/build-rootfs.sh", args
        source = Path(args[1])
        assert source.name == EXECUTOR and source.is_file(), args
        shutil.copyfile(source, source.parent / "executor")
        (source.parent / "executor").chmod(0o755)
    elif command == "file":
        assert Path(args[0]) == root / "build-elfloader/workerd-executor/executor", args
        assert Path(args[0]).is_file()
        print("ELF 64-bit static PIE")
    elif command == "readelf":
        assert args == ["-l", str(root / "build-elfloader/workerd-executor/executor")], args
        if os.environ.get("SETUP_READELF_ERROR"):
            print("synthetic readelf failure", file=sys.stderr)
            return 1
        print("Type: DYN")
    elif command == "wasm-tools":
        if args == ["--version"]:
            print("wasm-tools 1.252.0")
        elif args[0] == "parse":
            Path(args[args.index("-o") + 1]).write_bytes(b"fixture")
        else:
            assert args[0] == "validate", args
    elif command == "node":
        if args == ["--version"]:
            print("v24.0.0")
        else:
            assert args[0] == "--test", args
    elif command in ("rustup", "cargo", "just", "llvm-strip"):
        pass
    else:
        raise AssertionError(f"unexpected external execution: {command} {args}")
    return 0


class SetupTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="workerd-setup-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "tools/lib").mkdir(parents=True)
        for name in ("setup-workerd-demo.sh", "lib/go-toolchain.sh"):
            shutil.copyfile(ROOT / "tools" / name, self.root / "tools" / name)
        (self.root / "workerd/.git").mkdir(parents=True)
        (self.root / "workerd/.devcontainer").mkdir()
        (self.root / "experiments/workerd-component-model").mkdir(parents=True)
        self.bin = self.root / "stub-bin"
        self.bin.mkdir()
        for command in (
            "id", "git", "docker", "bazel", "clang++", "dpkg-query",
            "bash", "file", "readelf", "wasm-tools", "node", "rustup",
            "cargo", "just", "llvm-strip", "hey", "vegeta", "go",
            "curl", "sqlite3", "patch",
        ):
            (self.bin / command).symlink_to(Path(__file__).resolve())
        # Hardware is outside this test: only its three builtin checks are stubbed.
        prelude = self.root / "bash-env"
        prelude.write_text(
            'test() { if [[ "$#" == 2 && "$2" == /dev/kvm ]]; then return 0; '
            'else builtin test "$@"; fi; }\n'
        )
        self.env = {
            **os.environ, "HOME": str(self.root / "home"),
            "CARGO_HOME": str(self.root / "home/.cargo"),
            "RUSTUP_HOME": str(self.root / "home/.rustup"),
            "XDG_CACHE_HOME": str(self.root / "cache"),
            "WORKERD_DIR": str(self.root / "workerd"),
            "PATH": str(self.bin) + ":/usr/bin:/bin",
            "BASH_ENV": str(prelude), "SETUP_STUB_ROOT": str(self.root),
        }
        for name in ("DOCKER_HOST", "DOCKER_CONTEXT"):
            self.env.pop(name, None)

    def run_setup(self, **variables):
        return subprocess.run(
            ["/bin/bash", str(self.root / "tools/setup-workerd-demo.sh")],
            cwd=self.root, env={**self.env, **variables}, text=True,
            capture_output=True, timeout=20, check=False,
        )

    def trace(self):
        return [json.loads(line) for line in (self.root / "trace.jsonl").read_text().splitlines()]

    def test_build_install_package_and_reuse_exact_paths(self):
        result = self.run_setup()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        expected = self.root / "build-elfloader/workerd-executor/executor"
        self.assertIn(f"Packaged executor:\n  {expected}\n", result.stdout)
        self.assertEqual(
            [record[1] for record in self.trace() if record[0] == "file"],
            [str(expected)],
        )
        self.assertTrue((expected.parent / EXECUTOR).is_file())
        self.assertEqual(len((self.root / "self-tests").read_text().splitlines()), 2)
        repeated = self.run_setup()
        self.assertEqual(repeated.returncode, 0, repeated.stderr)
        self.assertIn("Reusing the validated Workerd executor", repeated.stdout)
        self.assertEqual(sum(record[0] == "bazel" for record in self.trace()), 1)

    def test_actual_access_accepts_rootless_without_docker_group(self):
        result = self.run_setup(
            DOCKER_HOST="unix:///run/user/1000/docker.sock",
            SETUP_SESSION_GROUPS="users", SETUP_ACCOUNT_GROUPS="users",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(["id", "-nG"], self.trace())

    def test_missing_and_pending_session_membership_are_actionable(self):
        for account, expected in [
            ("users", "Ask your administrator"),
            ("users docker", "account belongs to the docker group"),
        ]:
            with self.subTest(account=account):
                result = self.run_setup(
                    SETUP_DOCKER_ERROR="permission denied opening Docker socket",
                    SETUP_SESSION_GROUPS="users", SETUP_ACCOUNT_GROUPS=account,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected, result.stderr)
                self.assertIn("new login session", result.stderr)
                self.assertFalse((self.root / "builder-ready").exists())

    def test_active_group_permission_error_does_not_recommend_rejoining(self):
        result = self.run_setup(SETUP_DOCKER_ERROR="permission denied opening Docker socket")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("docker group is already active", result.stderr)
        self.assertIn("socket ownership/ACLs", result.stderr)

    def test_unavailable_daemon_preserves_actual_error(self):
        result = self.run_setup(SETUP_DOCKER_ERROR="Cannot connect to the Docker daemon")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Cannot connect to the Docker daemon", result.stderr)
        self.assertIn("selected context/DOCKER_HOST", result.stderr)
        self.assertNotIn("new login session", result.stderr)

    def test_remote_permission_error_does_not_require_local_group(self):
        result = self.run_setup(
            DOCKER_HOST="ssh://docker.example.test",
            SETUP_DOCKER_ERROR="permission denied by remote SSH endpoint",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("rootless or remote Docker endpoint", result.stderr)
        self.assertNotIn(["id", "-nG"], self.trace())

    def test_missing_build_output_and_failed_elf_inspection_fail_closed(self):
        for variable in ("SETUP_MISSING_EXECUTOR", "SETUP_READELF_ERROR"):
            with self.subTest(variable=variable):
                result = self.run_setup(**{variable: "1"})
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("Setup complete.", result.stdout)
                if variable == "SETUP_READELF_ERROR":
                    self.assertIn("could not inspect the packaged executor", result.stderr)
                else:
                    self.assertIn(EXECUTOR, result.stderr)


if __name__ == "__main__":
    if os.environ.get("SETUP_STUB_ROOT") and Path(sys.argv[0]).name != Path(__file__).resolve().name:
        sys.exit(stub())
    unittest.main()

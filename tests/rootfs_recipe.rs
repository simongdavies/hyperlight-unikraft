// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

const JUSTFILE: &str = include_str!("../justfile");
const TEST_HELPERS: &str = include_str!("common/mod.rs");

#[test]
fn agent_custom_recipe_resolves_checked_in_source_and_base() {
    for expected in [
        r#"elif [ "{{runtime}}" = "agent-custom" ]; then"#,
        r#"df="{{root_dir}}/examples/agent/custom/Dockerfile""#,
        "just build-rootfs python-shell",
        "docker image inspect hluk-python-shell-rootfs:latest >/dev/null",
    ] {
        assert!(
            JUSTFILE.contains(expected),
            "missing agent-custom recipe contract: {expected}"
        );
    }
}

#[test]
fn missing_agent_custom_fixture_reports_reproducible_command() {
    assert!(
        TEST_HELPERS.contains("`just build-rootfs agent-custom examples/agent/custom/Dockerfile`")
    );
}

#!/usr/bin/env python3
"""Native failure-path tests for the exact typed-v1 helper in the kernel patch."""
import pathlib
import subprocess
import tempfile

root = pathlib.Path(__file__).resolve().parent.parent
patch = (root / "kernel/patches/hyperlight-hostcall-ioctl.patch").read_text()
added = "".join(line[1:] for line in patch.splitlines(keepends=True)
                if line.startswith("+") and not line.startswith("+++"))
helper = added[added.index("static int hl_typed_host_call_v1("):]
helper = helper.split("\n\tcase HLCALL_IOC_TYPED_HOSTCALL_V1:")[0]

prefix = r"""
#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "hl_driver.h"
typedef uint8_t __u8;
typedef uint32_t __u32;
typedef uint64_t __u64;
typedef int32_t __s32;
typedef size_t __sz;
#define HL_PV_HLINT 1
#define HL_PV_HLULONG 4
#define HL_PV_HLSTRING 7
#define HL_PV_HLVECBYTES 9
struct hl_param {
    __u8 type;
    union {
        __s32 i32_val;
        __u64 u64_val;
        struct { const char *ptr; __u32 len; } str;
        struct { const __u8 *ptr; __u32 len; } vec;
    };
};
static __u8 arguments[128], reply[128];
static __u8 *hl_args_buf = arguments, *hl_reply_buf = reply;
static __u64 hl_reply_cap = sizeof(reply), hl_call_cap = 128;
static int hl_call_in_flight = 1, calls, refused;
static __u64 hl_hcall_max_payload(void) { return 64; }
static int hl_utf8_valid(const __u8 *s, __u64 len) {
    for (__u64 i = 0; i < len; i++) if (s[i] >= 128) return 0;
    return 1;
}
static int check(const char *name, const struct hl_param *p, int n) {
    assert(!strcmp(name, "fixture"));
    if (n) {
        assert(n == 1);
        assert(p[0].type == HL_PV_HLSTRING || p[0].type == HL_PV_HLVECBYTES);
        assert(p[0].vec.len == 3 && !memcmp(p[0].vec.ptr, "abc", 3));
    }
    calls++;
    return refused ? -2 : 0;
}
static int hl_hcall_int(const char *name, const struct hl_param *p, int n, __s32 *out) {
    *out = 17; return check(name, p, n);
}
static int hl_hcall_ulong(const char *name, const struct hl_param *p, int n, __u64 *out) {
    *out = 23; return check(name, p, n);
}
static int hl_hcall_string(const char *name, const struct hl_param *p, int n,
                          char *out, __sz cap, __sz *len) {
    assert(cap >= 3); memcpy(out, "xyz", 3); *len = 3; return check(name, p, n);
}
static int hl_hcall_vecbytes(const char *name, const struct hl_param *p, int n,
                            __u8 *out, __sz cap, __sz *len) {
    return hl_hcall_string(name, p, n, (char *)out, cap, len);
}
"""
tests = r"""
static struct hlcall_host_call valid(void) {
    return (struct hlcall_host_call){
        .version=1, .function="fixture", .function_len=7,
        .return_type=HLCALL_HOST_I32
    };
}
static void denied(struct hlcall_host_call call, int error) {
    int before = calls;
    assert(hl_typed_kernel_call(&call) == error);
    assert(calls == before);
}
int main(void) {
    assert(HLCALL_IOC_HOSTCALL == 0xc0384803UL);
    assert(HLCALL_IOC_TYPED_HOSTCALL_V1 == 0xc0c84803UL);
    struct hlcall_host_call c = valid();
    hl_call_in_flight=0; denied(c, EPERM); hl_call_in_flight=1;
    c.version=2; denied(c, EINVAL);
    c=valid(); c.reserved=1; denied(c, EINVAL);
    c=valid(); c.reserved2=1; denied(c, EINVAL);
    c=valid(); c.function_len=129; denied(c, EINVAL);
    c=valid(); c.function_len=65; denied(c, E2BIG);
    c=valid(); c.function="bad\0name"; c.function_len=8; denied(c, EINVAL);
    c=valid(); c.function="\xff"; c.function_len=1; denied(c, EILSEQ);
    c=valid(); c.arg_count=5; denied(c, EINVAL);
    c=valid(); c.arg_count=1; c.args[0].type=99; denied(c, EINVAL);
    c=valid(); c.arg_count=1; c.args[0].reserved=1; denied(c, EINVAL);
    c=valid(); c.arg_count=1; c.args[0].type=HLCALL_HOST_VECBYTES;
    c.args[0].data="abc"; c.args[0].len=58; denied(c, E2BIG);
    c.args[0].len=UINT64_MAX; denied(c, EINVAL);
    c.args[0].len=3; c.args[0].data=NULL; denied(c, EINVAL);
    c.args[0].type=HLCALL_HOST_STRING; c.args[0].data="\xffxx"; denied(c, EILSEQ);
    c=valid(); c.output=reply; denied(c, EINVAL);
    c=valid(); c.return_type=HLCALL_HOST_STRING; denied(c, EINVAL);
    c.output=reply; c.output_cap=127; denied(c, EINVAL);
    c=valid(); hl_args_buf=NULL; denied(c, ENOMEM); hl_args_buf=arguments;
    c=valid(); refused=1; assert(hl_typed_kernel_call(&c)==EIO); refused=0;
    c=valid(); assert(!hl_typed_kernel_call(&c) && c.output_i32==17);
    c=valid(); c.return_type=HLCALL_HOST_U64;
    assert(!hl_typed_kernel_call(&c) && c.output_u64==23);
    for (int type=HLCALL_HOST_STRING; type<=HLCALL_HOST_VECBYTES; type++) {
        char output[128]={0};
        c=valid(); c.return_type=type; c.output=output; c.output_cap=128;
        c.arg_count=1; c.args[0].type=type; c.args[0].data="abc"; c.args[0].len=3;
        assert(!hl_typed_kernel_call(&c));
        assert(c.output_len==3 && !memcmp(output, "xyz", 3));
    }
    puts("PASS: exact typed-v1 kernel helper failure paths, scalar/string/external-byte replies, distinct ioctl layouts");
}
"""
# The driver header has its own ioctl wrapper; test the kernel helper separately.
helper = helper.replace("hl_typed_host_call_v1(", "hl_typed_kernel_call(")
with tempfile.TemporaryDirectory(prefix="hyperloom-typed-abi-") as directory:
    source = pathlib.Path(directory) / "test.c"
    binary = pathlib.Path(directory) / "test"
    source.write_text(prefix + helper + tests)
    subprocess.run(["cc", "-std=c11", "-D_GNU_SOURCE", "-Wall", "-Wextra",
                    "-Werror", "-Wno-unused-variable", "-I", str(root / "drivers"),
                    str(source), "-o", str(binary)], check=True)
    subprocess.run([str(binary)], check=True)

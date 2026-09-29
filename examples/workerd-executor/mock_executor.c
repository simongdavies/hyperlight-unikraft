// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

#define _GNU_SOURCE
#include "hl_driver.h"

static int initialized;

static int dispatch(const uint8_t *fc, size_t fc_len)
{
	size_t len;
	const char *arg = fc_arg0_string(fc, fc_len, &len);
	if (!arg)
		return -1;
	if (fc_name_is(fc, fc_len, "init")) {
		if (len > 60 * 1024 ||
		    !strstr(arg, "\"protocol_version\":1") ||
		    !strstr(arg, "\"worker_version\":") ||
		    !strstr(arg, "\"main_module\":") ||
		    !strstr(arg, "\"modules\":"))
			return -1;
		initialized = 1;
		return 0;
	}
	if (!initialized || !fc_name_is(fc, fc_len, "fetch"))
		return -1;
	char *json = strndup(arg, len);
	if (!json)
		return -1;
	const char *start = strstr(json, "\"request_id\":\"");
	char id[65];
	if (!start) {
		free(json);
		return -1;
	}
	start += strlen("\"request_id\":\"");
	size_t n = strcspn(start, "\"");
	if (!n || n >= sizeof(id)) {
		free(json);
		return -1;
	}
	memcpy(id, start, n);
	id[n] = 0;
	if (strstr(json, "/busy\"")) {
		for (;;)
			__asm__ volatile("" ::: "memory");
	}
	if (strstr(json, "/sleep\""))
		sleep(60);
	if (strstr(json, "/stale\""))
		strcpy(id, "stale");
	char response[512];
	int size = snprintf(response, sizeof(response),
		"{\"protocol_version\":1,\"request_id\":\"%s\","
		"\"status\":200,\"headers\":[],\"body_base64\":\"b2s=\"}\n", id);
	int result = 0;
	if (strstr(json, "/oversized\"")) {
		char block[4096];
		memset(block, 'x', sizeof(block));
		for (int i = 0; i < 16; i++)
			if (hl_write_all(STDOUT_FILENO, block, sizeof(block)))
				result = -1;
	} else if (strstr(json, "/malformed\"")) {
		result = hl_write_all(STDOUT_FILENO, "{bad}\n", 6);
	} else if (strstr(json, "/fragmented\"")) {
		size_t body_len = 8192;
		size_t capacity = body_len + 256;
		char *large = malloc(capacity);
		if (!large) {
			result = -1;
		} else {
			int prefix = snprintf(large, capacity,
				"{\"protocol_version\":1,\"request_id\":\"%s\","
				"\"status\":200,\"headers\":[],\"body_base64\":\"", id);
			if (prefix < 0 || (size_t)prefix + body_len + 4 > capacity) {
				result = -1;
			} else {
				memset(large + prefix, 'Y', body_len);
				size_t total = (size_t)prefix + body_len;
				memcpy(large + total, "\"}\n", 3);
				total += 3;
				for (size_t offset = 0; offset < total; offset += 2048) {
					size_t part = total - offset;
					if (part > 2048)
						part = 2048;
					if (hl_write_all(STDOUT_FILENO, large + offset, part)) {
						result = -1;
						break;
					}
				}
			}
			free(large);
		}
	} else {
		if (strstr(json, "/prefix\""))
			result = hl_write_all(STDOUT_FILENO, "log\n", 4);
		if (hl_write_all(STDOUT_FILENO, response, (size_t)size))
			result = -1;
		if (strstr(json, "/duplicate\"") || strstr(json, "/suffix\""))
			if (hl_write_all(STDOUT_FILENO, response, (size_t)size))
				result = -1;
	}
	free(json);
	return result;
}

int main(void)
{
	if (hl_driver_init("workerd-executor"))
		return 1;
	hl_driver_run(dispatch);
}

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

#define _GNU_SOURCE
#include "hl_driver.h"

static int initialized;
static unsigned int fetch_count;

static int broker_fetch(const char *request_json, const char *id, int post)
{
	const char *url = strstr(request_json,
		"\"name\":\"x-fetch-url\",\"value\":\"");
	char target[1024];
	char metadata[1400];
	char *result = NULL;
	uint8_t *bytes = NULL;
	uint64_t operation_id;
	size_t result_len;
	int rc = -1;

	if (!url)
		return -1;
	url += strlen("\"name\":\"x-fetch-url\",\"value\":\"");
	size_t target_len = strcspn(url, "\"");
	if (!target_len || target_len >= sizeof(target))
		return -1;
	memcpy(target, url, target_len);
	target[target_len] = 0;

	const char *method = post ? "POST" : "GET";
	const char *body = post ? "post" : "";
	size_t body_len = strlen(body);
	int metadata_len = snprintf(metadata, sizeof(metadata),
		"{\"protocol_version\":1,\"request_id\":\"outbound-%s\","
		"\"method\":\"%s\",\"url\":\"%s\","
		"\"header_block_length\":2,\"body_length\":%zu}",
		id, method, target, body_len);
	if (metadata_len < 0 || (size_t)metadata_len >= sizeof(metadata))
		return -1;

	size_t cap = hl_host_call_max_payload();
	result = malloc(cap + 1);
	bytes = malloc(cap);
	if (!result || !bytes)
		goto out;

	struct hlcall_host_arg start_arg = {
		.type = HLCALL_HOST_STRING,
		.data = metadata,
		.len = (uint64_t)metadata_len,
	};
	if (hl_host_call_string("WorkerdFetchV1Start", &start_arg, 1,
				result, cap, &result_len))
		goto out;
	if (result_len >= cap)
		goto out;
	result[result_len] = 0;
	const char *operation = strstr(result, "\"operation_id\":");
	if (!operation)
		goto out;
	operation_id = strtoull(operation + strlen("\"operation_id\":"), NULL, 10);
	if (!operation_id)
		goto out;

	char upload[6] = "[]";
	memcpy(upload + 2, body, body_len);
	struct hlcall_host_arg write_args[2] = {
		{ .type = HLCALL_HOST_U64, .value = operation_id },
		{ .type = HLCALL_HOST_VECBYTES, .data = upload,
		  .len = 2 + body_len },
	};
	int32_t written;
	if (hl_host_call_i32("WorkerdFetchV1Write", write_args, 2, &written) ||
	    written != (int32_t)(2 + body_len))
		goto cancel;

	struct hlcall_host_arg id_arg = {
		.type = HLCALL_HOST_U64,
		.value = operation_id,
	};
	int32_t status;
	if (hl_host_call_i32("WorkerdFetchV1Finish", &id_arg, 1, &status) ||
	    status)
		goto cancel;
	for (int attempt = 0; attempt < 2000; attempt++) {
		if (hl_host_call_string("WorkerdFetchV1Poll", &id_arg, 1,
					result, cap, &result_len))
			goto cancel;
		if (result_len >= cap)
			goto cancel;
		result[result_len] = 0;
		if (strstr(result, "\"state\":\"complete\""))
			break;
		usleep(1000);
	}
	if (!strstr(result, "\"state\":\"complete\"") ||
	    !strstr(result, "\"error\":null") ||
	    !strstr(result, "\"status\":200"))
		goto cancel;

	const char *header_len = strstr(result, "\"header_block_length\":");
	const char *response_len = strstr(result, "\"body_length\":");
	if (!header_len || !response_len)
		goto cancel;
	size_t header_bytes =
		strtoull(header_len + strlen("\"header_block_length\":"),
			 NULL, 10);
	size_t response_bytes =
		strtoull(response_len + strlen("\"body_length\":"), NULL, 10);
	size_t expected = header_bytes + response_bytes;
	if (expected > cap)
		goto cancel;
	struct hlcall_host_arg read_args[2] = {
		{ .type = HLCALL_HOST_U64, .value = operation_id },
		{ .type = HLCALL_HOST_U64,
		  .value = cap < 60 * 1024 ? cap : 60 * 1024 },
	};
	size_t received;
	if (hl_host_call_vecbytes("WorkerdFetchV1Read", read_args, 2,
				  bytes, cap, &received))
		goto cancel;
	if (received != expected)
		goto cancel;
	if (response_bytes != (post ? 4 : 2) ||
	    memcmp(bytes + header_bytes, post ? "post" : "ok",
		   response_bytes))
		goto out;
	rc = 0;
	goto out;

cancel:
	(void)hl_host_call_i32("WorkerdFetchV1Cancel", &id_arg, 1, &status);
out:
	free(bytes);
	free(result);
	return rc;
}

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
	fetch_count++;
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
	if (strstr(json, "/delay\""))
		usleep(300000);
	if (strstr(json, "/stale\""))
		strcpy(id, "stale");
	int broker = strstr(json, "/broker\"") != NULL;
	int broker_post = strstr(json, "/broker-post\"") != NULL;
	if ((broker || broker_post) && broker_fetch(json, id, broker_post)) {
		free(json);
		return -1;
	}
	char response[512];
	const char *body = strstr(json, "/instance\"") ? "MQ==" :
		(broker_post ? "cG9zdA==" : "b2s=");
	int size = snprintf(response, sizeof(response),
		"{\"protocol_version\":1,\"request_id\":\"%s\","
		"\"status\":200,\"headers\":[],\"body_base64\":\"%s\"}\n", id, body);
	if (strstr(json, "/instance\"") && fetch_count != 1)
		size = snprintf(response, sizeof(response),
			"{\"protocol_version\":1,\"request_id\":\"%s\","
			"\"status\":500,\"headers\":[],\"body_base64\":\"cmV1c2Vk\"}\n", id);
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

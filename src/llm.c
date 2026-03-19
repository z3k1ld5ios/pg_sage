/*
 * llm.c — LLM interface using libcurl
 *
 * OpenAI-compatible API (works with Claude, OpenAI, Ollama, vLLM, OpenRouter).
 * AGPL-3.0 License
 */

#include "pg_sage.h"

#include <string.h>
#include <curl/curl.h>
#include "lib/stringinfo.h"
#include "utils/timestamp.h"

/* ----------------------------------------------------------------
 * curl write callback
 * ---------------------------------------------------------------- */
static size_t
write_callback(void *contents, size_t size, size_t nmemb, void *userp)
{
    StringInfo  buf = (StringInfo) userp;
    size_t      total = size * nmemb;

    appendBinaryStringInfo(buf, (const char *) contents, total);
    return total;
}

/* ----------------------------------------------------------------
 * JSON string escaping helper
 *
 * Escapes a C string for inclusion in a JSON string literal.
 * Caller must pfree the result.
 * ---------------------------------------------------------------- */
static char *
json_escape(const char *input)
{
    StringInfoData buf;

    if (input == NULL)
        return pstrdup("");

    initStringInfo(&buf);

    for (const char *p = input; *p; p++)
    {
        switch (*p)
        {
            case '"':
                appendStringInfoString(&buf, "\\\"");
                break;
            case '\\':
                appendStringInfoString(&buf, "\\\\");
                break;
            case '\b':
                appendStringInfoString(&buf, "\\b");
                break;
            case '\f':
                appendStringInfoString(&buf, "\\f");
                break;
            case '\n':
                appendStringInfoString(&buf, "\\n");
                break;
            case '\r':
                appendStringInfoString(&buf, "\\r");
                break;
            case '\t':
                appendStringInfoString(&buf, "\\t");
                break;
            default:
                if ((unsigned char) *p < 0x20)
                {
                    appendStringInfo(&buf, "\\u%04x", (unsigned int) *p);
                }
                else
                {
                    appendStringInfoChar(&buf, *p);
                }
                break;
        }
    }

    return buf.data;
}

/* ----------------------------------------------------------------
 * Simple JSON value extractor
 *
 * Finds "key":"value" or "key": value in a JSON blob and returns
 * the value.  For strings, the surrounding quotes are stripped.
 * Returns palloc'd string or NULL.
 * ---------------------------------------------------------------- */
static char *
json_extract_string(const char *json, const char *key)
{
    char        search[256];
    const char *pos;
    const char *start;
    const char *end;

    if (json == NULL || key == NULL)
        return NULL;

    /* Try "key":" first (string value) */
    snprintf(search, sizeof(search), "\"%s\"", key);
    pos = strstr(json, search);
    if (pos == NULL)
        return NULL;

    /* Advance past the key and the colon */
    pos += strlen(search);
    while (*pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r')
        pos++;
    if (*pos != ':')
        return NULL;
    pos++;
    while (*pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r')
        pos++;

    if (*pos == '"')
    {
        /* String value — find matching close quote, handling escapes */
        pos++;
        start = pos;

        StringInfoData valbuf;
        initStringInfo(&valbuf);

        while (*pos && !(*pos == '"' && *(pos - 1) != '\\'))
        {
            if (*pos == '\\' && *(pos + 1))
            {
                pos++;
                switch (*pos)
                {
                    case '"':  appendStringInfoChar(&valbuf, '"'); break;
                    case '\\': appendStringInfoChar(&valbuf, '\\'); break;
                    case 'n':  appendStringInfoChar(&valbuf, '\n'); break;
                    case 'r':  appendStringInfoChar(&valbuf, '\r'); break;
                    case 't':  appendStringInfoChar(&valbuf, '\t'); break;
                    case '/':  appendStringInfoChar(&valbuf, '/'); break;
                    default:
                        appendStringInfoChar(&valbuf, '\\');
                        appendStringInfoChar(&valbuf, *pos);
                        break;
                }
            }
            else
            {
                appendStringInfoChar(&valbuf, *pos);
            }
            pos++;
        }

        return valbuf.data;
    }
    else if (*pos == 'n' && strncmp(pos, "null", 4) == 0)
    {
        return NULL;
    }
    else
    {
        /* Numeric or boolean — read until comma, brace, or bracket */
        start = pos;
        end = pos;
        while (*end && *end != ',' && *end != '}' && *end != ']'
               && *end != ' ' && *end != '\n' && *end != '\r')
            end++;

        return pnstrdup(start, end - start);
    }
}

/* ----------------------------------------------------------------
 * Extract a nested string: find an object containing anchor_key,
 * then extract target_key from within it.
 * ---------------------------------------------------------------- */
static char *
json_extract_nested_string(const char *json, const char *anchor_key,
                           const char *target_key)
{
    char        search[256];
    const char *pos;

    if (json == NULL)
        return NULL;

    snprintf(search, sizeof(search), "\"%s\"", anchor_key);
    pos = strstr(json, search);
    if (pos == NULL)
        return NULL;

    /* Now search for target_key starting from anchor position */
    return json_extract_string(pos, target_key);
}

/* ----------------------------------------------------------------
 * sage_llm_available
 * ---------------------------------------------------------------- */
bool
sage_llm_available(void)
{
    /* Must be enabled */
    if (!sage_llm_enabled)
        return false;

    /* Endpoint must be configured */
    if (sage_llm_endpoint == NULL || sage_llm_endpoint[0] == '\0')
        return false;

    /* API key must be set unless endpoint is localhost (Ollama) */
    if (sage_llm_api_key == NULL || sage_llm_api_key[0] == '\0')
    {
        if (strstr(sage_llm_endpoint, "localhost") == NULL &&
            strstr(sage_llm_endpoint, "127.0.0.1") == NULL)
            return false;
    }

    /* LLM circuit breaker must allow calls */
    if (!sage_llm_circuit_check())
        return false;

    /* Check daily token budget */
    if (sage_state != NULL)
    {
        int today;

        LWLockAcquire(sage_state->lock, LW_SHARED);
        today = sage_state->llm_day_of_year;

        if (sage_state->llm_tokens_used_today >= sage_llm_token_budget)
        {
            LWLockRelease(sage_state->lock);
            elog(DEBUG1, "pg_sage: LLM daily token budget exhausted (%d/%d)",
                 sage_state->llm_tokens_used_today, sage_llm_token_budget);
            return false;
        }
        LWLockRelease(sage_state->lock);
    }

    return true;
}

/* ----------------------------------------------------------------
 * sage_llm_call
 *
 * Makes an HTTP POST to the OpenAI-compatible chat completions
 * endpoint.  Returns a palloc'd content string on success, or
 * NULL on failure.  *tokens_used is set if non-NULL.
 * ---------------------------------------------------------------- */
char *
sage_llm_call(const char *system_prompt, const char *user_prompt,
              int max_tokens, int *tokens_used)
{
    CURL               *curl;
    CURLcode            res;
    struct curl_slist   *headers = NULL;
    StringInfoData      response_buf;
    StringInfoData      request_body;
    char                url[1024];
    char                auth_header[512];
    char               *escaped_system;
    char               *escaped_user;
    char               *escaped_model;
    char               *content = NULL;
    int                 attempt;
    long                http_code;
    int                 local_tokens = 0;

    if (tokens_used)
        *tokens_used = 0;

    if (!sage_llm_available())
        return NULL;

    /* Build URL */
    if (strstr(sage_llm_endpoint, "/chat/completions"))
        snprintf(url, sizeof(url), "%s", sage_llm_endpoint);
    else if (strstr(sage_llm_endpoint, "/v1"))
        snprintf(url, sizeof(url), "%s/chat/completions", sage_llm_endpoint);
    else
        snprintf(url, sizeof(url), "%s/v1/chat/completions", sage_llm_endpoint);

    /* Escape strings for JSON */
    escaped_system = json_escape(system_prompt);
    escaped_user   = json_escape(user_prompt);
    escaped_model  = json_escape(sage_llm_model);

    /* Build JSON request body */
    initStringInfo(&request_body);
    appendStringInfo(&request_body,
                     "{\"model\":\"%s\","
                     "\"messages\":["
                     "{\"role\":\"system\",\"content\":\"%s\"},"
                     "{\"role\":\"user\",\"content\":\"%s\"}"
                     "],"
                     "\"max_tokens\":%d,"
                     "\"temperature\":0.3}",
                     escaped_model,
                     escaped_system,
                     escaped_user,
                     max_tokens);

    pfree(escaped_system);
    pfree(escaped_user);
    pfree(escaped_model);

    /* Retry loop: 3 attempts with exponential backoff (1s, 4s, 16s) */
    for (attempt = 0; attempt < 3; attempt++)
    {
        if (attempt > 0)
        {
            /* Exponential backoff: 1s, 4s, 16s (in microseconds) */
            long backoff_us;

            if (attempt == 1)
                backoff_us = 1000000L;      /* 1 second */
            else
                backoff_us = (attempt == 2) ? 4000000L : 16000000L;

            elog(DEBUG1, "pg_sage: LLM retry %d, backing off %ld ms",
                 attempt, backoff_us / 1000);
            pg_usleep(backoff_us);
        }

        curl = curl_easy_init();
        if (curl == NULL)
        {
            elog(WARNING, "pg_sage: curl_easy_init() failed");
            continue;
        }

        /* Set up headers */
        headers = NULL;
        headers = curl_slist_append(headers, "Content-Type: application/json");

        if (sage_llm_api_key != NULL && sage_llm_api_key[0] != '\0')
        {
            snprintf(auth_header, sizeof(auth_header),
                     "Authorization: Bearer %s", sage_llm_api_key);
            headers = curl_slist_append(headers, auth_header);
        }

        /* Prepare response buffer */
        initStringInfo(&response_buf);

        /* Configure curl */
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_body.data);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long) sage_llm_timeout);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_buf);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_USERAGENT, "pg_sage/" PG_SAGE_VERSION);

        /* Perform request */
        res = curl_easy_perform(curl);
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK)
        {
            elog(WARNING, "pg_sage: LLM request failed (attempt %d): %s",
                 attempt + 1, curl_easy_strerror(res));
            pfree(response_buf.data);
            continue;
        }

        if (http_code != 200)
        {
            elog(WARNING, "pg_sage: LLM returned HTTP %ld (attempt %d): %.200s",
                 http_code, attempt + 1, response_buf.data);
            pfree(response_buf.data);

            /* Don't retry on 4xx errors (client errors) except 429 (rate limit) */
            if (http_code >= 400 && http_code < 500 && http_code != 429)
                break;

            continue;
        }

        /* Parse response: extract choices[0].message.content */
        content = json_extract_nested_string(response_buf.data, "message", "content");

        if (content == NULL)
        {
            /* Try alternate path: some APIs nest differently */
            content = json_extract_string(response_buf.data, "content");
        }

        if (content == NULL)
        {
            elog(WARNING, "pg_sage: failed to parse LLM response: %.200s",
                 response_buf.data);
            pfree(response_buf.data);
            continue;
        }

        /* Extract token usage */
        {
            char *tokens_str = json_extract_string(response_buf.data, "total_tokens");

            if (tokens_str != NULL)
            {
                local_tokens = atoi(tokens_str);
                pfree(tokens_str);
            }
            else
            {
                /* Estimate tokens: roughly 4 chars per token */
                local_tokens = (int) (strlen(system_prompt) + strlen(user_prompt) +
                                      strlen(content)) / 4;
            }
        }

        pfree(response_buf.data);

        /* Success — break out of retry loop */
        break;
    }

    pfree(request_body.data);

    if (content == NULL)
    {
        /* All retries exhausted */
        sage_llm_circuit_record_failure();
        return NULL;
    }

    /* Record success in circuit breaker */
    sage_llm_circuit_record_success();

    /* Update token usage in shared state */
    if (sage_state != NULL && local_tokens > 0)
    {
        struct pg_tm    tm;
        fsec_t          fsec;
        int             today_doy;
        TimestampTz     now_ts = GetCurrentTimestamp();

        timestamp2tm(now_ts, NULL, &tm, &fsec, NULL, NULL);
        today_doy = tm.tm_yday;

        LWLockAcquire(sage_state->lock, LW_EXCLUSIVE);

        /* Reset counter if it's a new day */
        if (sage_state->llm_day_of_year != today_doy)
        {
            sage_state->llm_tokens_used_today = 0;
            sage_state->llm_day_of_year = today_doy;
        }

        sage_state->llm_tokens_used_today += local_tokens;
        LWLockRelease(sage_state->lock);
    }

    if (tokens_used)
        *tokens_used = local_tokens;

    elog(DEBUG1, "pg_sage: LLM call succeeded, %d tokens used", local_tokens);

    return content;
}

import json
import time
import requests
import ijson.backends.yajl2_c as ijson
import ijson as ijson_core
import singer

from tap_workday_raas.exceptions import WorkdayRaasAuthenticationError

LOGGER = singer.get_logger()

_AUTH_FAILURE_CODES = (401, 403)
_EXPIRY_BUFFER_SECS = 60  # refresh proactively 60 s before actual expiry


class WorkdayOAuthClient:
    """Manages OAuth 2.0 token lifecycle for Workday RaaS requests.

      - Token endpoint is derived from ``hostname`` + ``tenant`` when
        ``token_endpoint`` is not explicitly provided.
      - Refresh uses HTTP Basic auth for ``client_id``/``client_secret``.
      - Workday rotating refresh tokens are persisted back to the config
        file so the next tap process uses the updated token.
      - Use as a context manager: token is always refreshed on ``__enter__``.
      - ``get_access_token()`` proactively refreshes 60 s before expiry.
    """

    def __init__(self, config, config_path=None):
        self.config = config
        self._config_path = config_path
        # Derive token endpoint from hostname + tenant when not explicitly set.
        self._token_endpoint = config.get("token_endpoint") or (
            "https://{}/ccx/oauth2/{}/token".format(
                config["hostname"], config["tenant"]
            )
        )
        self._client_id = config["client_id"]
        self._client_secret = config["client_secret"]
        self._refresh_token = config["refresh_token"]
        self._access_token = None
        self._expires_at = 0.0

    def __enter__(self):
        self._refresh_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        pass  # no persistent session to close

    def _refresh_access_token(self) -> None:
        """Exchange the refresh token for a new access token using HTTP Basic
        authentication for client credentials.
        """
        LOGGER.info("Refreshing OAuth access token.")
        try:
            resp = requests.post(
                self._token_endpoint,
                auth=(self._client_id, self._client_secret),
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._refresh_token,
                },
                timeout=30,
            )
        except requests.RequestException as exc:
            raise WorkdayRaasAuthenticationError(
                "OAuth token request failed (network error): {}".format(exc)
            ) from exc

        if resp.status_code == 401:
            raise WorkdayRaasAuthenticationError(
                "OAuth token request rejected (HTTP 401): "
                "verify client_id, client_secret, and refresh_token in the tap config."
            )
        if not resp.ok:
            raise WorkdayRaasAuthenticationError(
                "OAuth token request failed (HTTP {}): {}".format(
                    resp.status_code, resp.text[:300]
                )
            )

        token_data = resp.json()
        new_token = token_data.get("access_token")
        if not new_token:
            raise Exception(
                "Token endpoint response is missing the 'access_token' field."
            )
        expires_in = int(token_data.get("expires_in", 3600))
        self._access_token = new_token
        self._expires_at = time.monotonic() + expires_in

        # Workday rotates refresh tokens: persist the new one immediately so the
        # next tap process reads the valid token from the config file.
        new_refresh_token = token_data.get("refresh_token")
        if new_refresh_token:
            self._refresh_token = new_refresh_token
            self.config["refresh_token"] = new_refresh_token
            self._persist_config()

        LOGGER.info("OAuth access token refreshed successfully.")

    def _persist_config(self) -> None:
        """Write the current config (including rotated refresh_token) back to disk."""
        if not self._config_path:
            LOGGER.debug("No config_path set; skipping refresh token persistence to disk")
            return
        try:
            with open(self._config_path, "w") as fh:
                json.dump(self.config, fh, indent=2)
            LOGGER.debug("Persisted rotated refresh token to %s", self._config_path)
        except OSError as exc:
            LOGGER.warning("Failed to persist rotated refresh token: %s", exc)

    def get_access_token(self) -> str:
        """Return a valid access token, refreshing proactively when near expiry."""
        if (
            self._access_token
            and time.monotonic() < self._expires_at - _EXPIRY_BUFFER_SECS
        ):
            return self._access_token
        self._refresh_access_token()
        return self._access_token

    def _auth_headers(self):
        return {"Authorization": "Bearer {}".format(self.get_access_token())}

    def get(self, url, **kwargs):
        """Make an authenticated GET request, retrying once on 401/403."""
        resp = requests.get(url, headers=self._auth_headers(), **kwargs)
        if resp.status_code in _AUTH_FAILURE_CODES:
            LOGGER.info(
                "Access token rejected (HTTP %s). Attempting token refresh.",
                resp.status_code,
            )
            self._refresh_access_token()
            resp = requests.get(url, headers=self._auth_headers(), **kwargs)
        return resp


def stream_report(report_url, auth_client):
    # Force the format query param to be set to format=json

    # Split query params off
    url_breakdown = report_url.split("?")

    # Gather all params that are not format
    if len(url_breakdown) == 1:
        params = []
    else:
        params = [x for x in url_breakdown[1].split("&") if not x.startswith("format=")]

    # Add the format param
    params.append("format=json")
    param_string = "&".join(params)

    # Put the url back together
    corrected_url = url_breakdown[0] + "?" + param_string

    # Open authenticated streaming request.  Authentication is checked before
    # any response body is consumed so that we can safely retry with a
    # refreshed token without risking a partially-consumed stream.
    def _open_stream():
        return requests.get(
            corrected_url,
            headers={"Authorization": "Bearer {}".format(auth_client.get_access_token())},
            stream=True,
        )

    resp = _open_stream()
    if resp.status_code in _AUTH_FAILURE_CODES:
        resp.close()
        LOGGER.info(
            "Access token rejected during report request (HTTP %s). "
            "Attempting token refresh.",
            resp.status_code,
        )
        auth_client._refresh_access_token()
        resp = _open_stream()

    with resp:
        if not resp.ok:
            # Force-read the error response body before raise_for_status() closes
            # the connection, so e.response.text is accessible to callers.
            _ = resp.content
        resp.raise_for_status()

        # Set up our search key
        report_entry_key = b"Report_Entry"
        search_prefix = report_entry_key.decode("utf-8") + ".item"

        # NB This creates a "push" style interface with the ijson iterable
        # parser This sendable_list will be populated with intermediate
        # values by the items_coro() when send() is called. The
        # sendable_list must then be purged of values before it can be
        # used again. We have an explicit check for whether we find the
        # 'Report_Entry' key because if we do not find it the parser
        # yields 0 records instead of failing and this allows us to know
        # if the schema is changed
        records = ijson_core.sendable_list()
        coro = ijson.items_coro(records, search_prefix)

        # Track key presence using an ijson event parser so we are not
        # vulnerable to the key name being split across chunk boundaries
        # (raw byte scanning of small chunks could miss it).
        key_events = ijson_core.sendable_list()
        key_coro = ijson.parse_coro(key_events)
        found_report_entry = False
        response_is_json_object = False

        for chunk in resp.iter_content(chunk_size=512):
            coro.send(chunk)
            key_coro.send(chunk)

            # Scan parser events returned so far for the keys we care about
            for prefix, event, _value in key_events:
                if prefix == "" and event == "start_map":
                    response_is_json_object = True
                elif prefix == "" and event == "map_key" and _value == report_entry_key.decode("utf-8"):
                    found_report_entry = True
            del key_events[:]

            for rec in records:
                yield rec
            del records[:]

        if not found_report_entry:
            if response_is_json_object:
                # The response is valid JSON but does not contain the
                # Report_Entry key.  This is the standard Workday zero-row
                # response – log a warning and continue.
                LOGGER.warning(
                    "Did not see '%s' key in response. "
                    "Report returned 0 rows (empty result set).",
                    report_entry_key.decode("utf-8"),
                )
            else:
                # Unexpected payload – the response is not even a JSON
                # object.  This likely indicates an API error.
                raise Exception(
                    "Did not see '{}' key in response. "
                    "Report does not conform to expected schema, failing."
                    .format(report_entry_key.decode("utf-8"))
                )

        coro.close()
        key_coro.close()


def download_xsd(report_url, auth_client):
    if "?" in report_url:
        xsds_url = report_url.split("?")[0] + "?xsds"
    else:
        xsds_url = report_url + "?xsds"
    response = auth_client.get(xsds_url)
    response.raise_for_status()

    return response.text

import json
import time
import unittest
from unittest.mock import patch, MagicMock, mock_open

import requests

from tap_workday_raas.client import WorkdayOAuthClient, stream_report, download_xsd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(**config_overrides):
    """Return a WorkdayOAuthClient pre-loaded with a valid in-memory token."""
    cfg = {
        "hostname": "test.workday.com",
        "tenant": "mytenant",
        "client_id": "test-client-id",
        "client_secret": "test-client-secret",
        "refresh_token": "test-refresh-token",
    }
    cfg.update(config_overrides)
    client = WorkdayOAuthClient(cfg)
    client._access_token = "test-access-token"
    client._expires_at = time.monotonic() + 86400
    return client


def _oauth_config(extra=None):
    """Return a minimal valid OAuth config dict using hostname + tenant."""
    cfg = {
        "hostname": "test.workday.com",
        "tenant": "mytenant",
        "client_id": "test-client-id",
        "client_secret": "test-client-secret",
        "refresh_token": "test-refresh-token",
        "reports": "[]",
    }
    if extra:
        cfg.update(extra)
    return cfg


def _make_refresh_response(access_token="refreshed-token", expires_in=3600,
                            new_refresh_token=None):
    """Build a mock successful token-endpoint POST response."""
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.status_code = 200
    body = {"access_token": access_token, "expires_in": expires_in}
    if new_refresh_token:
        body["refresh_token"] = new_refresh_token
    mock_resp.json.return_value = body
    return mock_resp


def _make_ok_streaming_response(body_bytes):
    """Build a mock streaming response that yields body_bytes as a single chunk."""
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.status_code = 200
    mock_resp.iter_content.return_value = [body_bytes]
    mock_resp.raise_for_status = MagicMock()
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


def _make_error_response(status_code):
    """Build a mock non-streaming error response."""
    mock_resp = MagicMock()
    mock_resp.ok = False
    mock_resp.status_code = status_code
    http_err = requests.exceptions.HTTPError(response=mock_resp)
    mock_resp.raise_for_status.side_effect = http_err
    mock_resp.content = b""
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


# ---------------------------------------------------------------------------
# Token endpoint derivation
# ---------------------------------------------------------------------------

class TestTokenEndpointDerivation(unittest.TestCase):
    """Token endpoint is derived from hostname + tenant when not explicitly set."""

    def test_derives_endpoint_from_hostname_and_tenant(self):
        client = WorkdayOAuthClient({
            "hostname": "acme.workday.com",
            "tenant": "acmecorp",
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt",
        })
        self.assertEqual(
            client._token_endpoint,
            "https://acme.workday.com/ccx/oauth2/acmecorp/token",
        )

    def test_explicit_token_endpoint_overrides_derivation(self):
        client = WorkdayOAuthClient({
            "hostname": "acme.workday.com",
            "tenant": "acmecorp",
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt",
            "token_endpoint": "https://custom.example.com/token",
        })
        self.assertEqual(client._token_endpoint, "https://custom.example.com/token")

    @patch("tap_workday_raas.client.requests.post")
    def test_refresh_posts_to_derived_endpoint(self, mock_post):
        mock_post.return_value = _make_refresh_response()
        client = WorkdayOAuthClient({
            "hostname": "acme.workday.com",
            "tenant": "acmecorp",
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt",
        })
        client._refresh_access_token()
        self.assertEqual(
            mock_post.call_args[0][0],
            "https://acme.workday.com/ccx/oauth2/acmecorp/token",
        )


# ---------------------------------------------------------------------------
# Client construction & context manager
# ---------------------------------------------------------------------------

class TestWorkdayOAuthClientConstruction(unittest.TestCase):
    """WorkdayOAuthClient takes a config dict; no access_token required."""

    def test_construction_stores_config(self):
        cfg = _oauth_config()
        client = WorkdayOAuthClient(cfg)
        self.assertIs(client.config, cfg)
        self.assertIsNone(client._access_token)
        self.assertEqual(client._expires_at, 0.0)

    @patch("tap_workday_raas.client.requests.post")
    def test_enter_refreshes_token(self, mock_post):
        """__enter__ always calls _refresh_access_token so the token is fresh."""
        mock_post.return_value = _make_refresh_response("entry-token")
        with WorkdayOAuthClient(_oauth_config()) as client:
            self.assertEqual(client._access_token, "entry-token")

    @patch("tap_workday_raas.client.requests.post")
    def test_enter_returns_self(self, mock_post):
        mock_post.return_value = _make_refresh_response()
        client = WorkdayOAuthClient(_oauth_config())
        result = client.__enter__()
        self.assertIs(result, client)

    def test_exit_does_not_raise(self):
        _make_client().__exit__(None, None, None)


# ---------------------------------------------------------------------------
# get_access_token - proactive expiry handling
# ---------------------------------------------------------------------------

class TestGetAccessToken(unittest.TestCase):
    """get_access_token returns cached token when valid; refreshes when near expiry."""

    def test_returns_cached_token_when_valid(self):
        client = _make_client()
        self.assertEqual(client.get_access_token(), "test-access-token")

    @patch("tap_workday_raas.client.requests.post")
    def test_refreshes_when_token_expired(self, mock_post):
        mock_post.return_value = _make_refresh_response("fresh-token")
        client = WorkdayOAuthClient(_oauth_config())
        client._access_token = "stale-token"
        client._expires_at = time.monotonic() - 100
        self.assertEqual(client.get_access_token(), "fresh-token")
        mock_post.assert_called_once()

    @patch("tap_workday_raas.client.requests.post")
    def test_refreshes_within_buffer_window(self, mock_post):
        """Token within 60 s of expiry triggers proactive refresh."""
        mock_post.return_value = _make_refresh_response("proactive-token")
        client = WorkdayOAuthClient(_oauth_config())
        client._access_token = "near-expiry-token"
        client._expires_at = time.monotonic() + 30  # < 60 s buffer
        self.assertEqual(client.get_access_token(), "proactive-token")
        mock_post.assert_called_once()

    @patch("tap_workday_raas.client.requests.post")
    def test_refreshes_when_no_token_yet(self, mock_post):
        mock_post.return_value = _make_refresh_response("initial-token")
        self.assertEqual(WorkdayOAuthClient(_oauth_config()).get_access_token(), "initial-token")
        mock_post.assert_called_once()

    @patch("tap_workday_raas.client.requests.post")
    def test_does_not_refresh_when_valid(self, mock_post):
        _make_client().get_access_token()
        mock_post.assert_not_called()

    @patch("tap_workday_raas.client.requests.post")
    def test_expires_at_set_after_refresh(self, mock_post):
        mock_post.return_value = _make_refresh_response(expires_in=3600)
        client = WorkdayOAuthClient(_oauth_config())
        before = time.monotonic()
        client.get_access_token()
        after = time.monotonic()
        self.assertGreater(client._expires_at, before + 3590)
        self.assertLess(client._expires_at, after + 3601)


# ---------------------------------------------------------------------------
# _refresh_access_token - HTTP Basic auth + grant_type
# ---------------------------------------------------------------------------

class TestRefreshAccessToken(unittest.TestCase):
    """_refresh_access_token uses HTTP Basic auth and refresh_token grant."""

    @patch("tap_workday_raas.client.requests.post")
    def test_uses_http_basic_auth(self, mock_post):
        """client_id and client_secret are sent via HTTP Basic auth, not in body."""
        mock_post.return_value = _make_refresh_response()
        client = WorkdayOAuthClient(_oauth_config())
        client._refresh_access_token()
        call_kwargs = mock_post.call_args[1]
        self.assertEqual(call_kwargs["auth"], ("test-client-id", "test-client-secret"))

    @patch("tap_workday_raas.client.requests.post")
    def test_sends_refresh_token_grant_in_body(self, mock_post):
        """grant_type=refresh_token and refresh_token are in the POST body."""
        mock_post.return_value = _make_refresh_response()
        client = WorkdayOAuthClient(_oauth_config())
        client._refresh_access_token()
        call_data = mock_post.call_args[1]["data"]
        self.assertEqual(call_data["grant_type"], "refresh_token")
        self.assertEqual(call_data["refresh_token"], "test-refresh-token")

    @patch("tap_workday_raas.client.requests.post")
    def test_client_credentials_not_in_body(self, mock_post):
        """client_id and client_secret must NOT appear in the POST body."""
        mock_post.return_value = _make_refresh_response()
        client = WorkdayOAuthClient(_oauth_config())
        client._refresh_access_token()
        call_data = mock_post.call_args[1]["data"]
        self.assertNotIn("client_id", call_data)
        self.assertNotIn("client_secret", call_data)

    @patch("tap_workday_raas.client.requests.post")
    def test_posts_to_derived_token_endpoint(self, mock_post):
        mock_post.return_value = _make_refresh_response()
        client = WorkdayOAuthClient(_oauth_config())
        client._refresh_access_token()
        self.assertEqual(
            mock_post.call_args[0][0],
            "https://test.workday.com/ccx/oauth2/mytenant/token",
        )

    @patch("tap_workday_raas.client.requests.post")
    def test_updates_access_token(self, mock_post):
        mock_post.return_value = _make_refresh_response("brand-new-token")
        client = WorkdayOAuthClient(_oauth_config())
        client._refresh_access_token()
        self.assertEqual(client._access_token, "brand-new-token")

    @patch("tap_workday_raas.client.requests.post")
    def test_401_raises_clear_error(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 401
        mock_post.return_value = mock_resp
        with self.assertRaises(Exception) as ctx:
            WorkdayOAuthClient(_oauth_config())._refresh_access_token()
        self.assertIn("401", str(ctx.exception))

    @patch("tap_workday_raas.client.requests.post")
    def test_failure_raises_without_exposing_secret(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 400
        mock_resp.text = ""
        mock_post.return_value = mock_resp
        with self.assertRaises(Exception) as ctx:
            WorkdayOAuthClient(_oauth_config())._refresh_access_token()
        err = str(ctx.exception)
        self.assertIn("400", err)
        self.assertNotIn("test-client-secret", err)
        self.assertNotIn("test-refresh-token", err)

    @patch("tap_workday_raas.client.requests.post")
    def test_network_error_raises(self, mock_post):
        mock_post.side_effect = requests.RequestException("connection refused")
        with self.assertRaises(Exception) as ctx:
            WorkdayOAuthClient(_oauth_config())._refresh_access_token()
        self.assertIn("network error", str(ctx.exception))

    @patch("tap_workday_raas.client.requests.post")
    def test_missing_access_token_in_response_raises(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"token_type": "Bearer"}
        mock_post.return_value = mock_resp
        with self.assertRaises(Exception) as ctx:
            WorkdayOAuthClient(_oauth_config())._refresh_access_token()
        self.assertIn("access_token", str(ctx.exception))


# ---------------------------------------------------------------------------
# Refresh token rotation
# ---------------------------------------------------------------------------

class TestRefreshTokenRotation(unittest.TestCase):
    """When the token response contains a new refresh_token it is persisted."""

    @patch("tap_workday_raas.client.requests.post")
    def test_rotation_updates_in_memory_refresh_token(self, mock_post):
        mock_post.return_value = _make_refresh_response(
            new_refresh_token="rotated-refresh-token"
        )
        client = WorkdayOAuthClient(_oauth_config())
        client._refresh_access_token()
        self.assertEqual(client._refresh_token, "rotated-refresh-token")

    @patch("tap_workday_raas.client.requests.post")
    def test_rotation_updates_config_dict(self, mock_post):
        mock_post.return_value = _make_refresh_response(
            new_refresh_token="rotated-refresh-token"
        )
        cfg = _oauth_config()
        client = WorkdayOAuthClient(cfg)
        client._refresh_access_token()
        self.assertEqual(cfg["refresh_token"], "rotated-refresh-token")

    @patch("builtins.open", new_callable=mock_open)
    @patch("tap_workday_raas.client.requests.post")
    def test_rotation_persists_to_config_file(self, mock_post, m_open):
        mock_post.return_value = _make_refresh_response(
            new_refresh_token="rotated-refresh-token"
        )
        cfg = _oauth_config()
        client = WorkdayOAuthClient(cfg, config_path="/tmp/config.json")
        client._refresh_access_token()
        m_open.assert_called_once_with("/tmp/config.json", "w")
        written = "".join(
            call.args[0] for call in m_open().write.call_args_list
        )
        persisted = json.loads(written)
        self.assertEqual(persisted["refresh_token"], "rotated-refresh-token")

    @patch("tap_workday_raas.client.requests.post")
    def test_no_rotation_leaves_refresh_token_unchanged(self, mock_post):
        """When no new refresh_token is in the response, existing one is kept."""
        mock_post.return_value = _make_refresh_response()
        cfg = _oauth_config()
        client = WorkdayOAuthClient(cfg)
        client._refresh_access_token()
        self.assertEqual(client._refresh_token, "test-refresh-token")

    @patch("tap_workday_raas.client.requests.post")
    def test_no_config_path_skips_file_write(self, mock_post):
        """When config_path is None, _persist_config does not attempt a file open."""
        mock_post.return_value = _make_refresh_response(new_refresh_token="rotated")
        client = WorkdayOAuthClient(_oauth_config(), config_path=None)
        client._refresh_access_token()
        self.assertEqual(client._refresh_token, "rotated")

    @patch("builtins.open", side_effect=OSError("disk full"))
    @patch("tap_workday_raas.client.requests.post")
    def test_persist_config_oserror_logs_warning_not_raises(self, mock_post, _m_open):
        """A disk-write failure should warn but not abort the tap."""
        mock_post.return_value = _make_refresh_response(new_refresh_token="rotated")
        client = WorkdayOAuthClient(_oauth_config(), config_path="/tmp/config.json")
        client._refresh_access_token()  # should not raise


# ---------------------------------------------------------------------------
# Bearer token in RaaS requests
# ---------------------------------------------------------------------------

class TestBearerTokenUsedInStreamReport(unittest.TestCase):
    """stream_report sends Authorization: Bearer <access_token>."""

    @patch("tap_workday_raas.client.requests.get")
    def test_bearer_token_present_in_request(self, mock_get):
        body = json.dumps({"Report_Entry": [{"col": "val"}]}).encode()
        mock_get.return_value = _make_ok_streaming_response(body)
        list(stream_report("http://fake", _make_client()))
        headers = mock_get.call_args[1].get("headers", {})
        self.assertEqual(headers.get("Authorization"), "Bearer test-access-token")

    @patch("tap_workday_raas.client.requests.get")
    def test_no_basic_auth_used(self, mock_get):
        body = json.dumps({"Report_Entry": []}).encode()
        mock_get.return_value = _make_ok_streaming_response(body)
        list(stream_report("http://fake", _make_client()))
        self.assertNotIn("auth", mock_get.call_args[1])


# ---------------------------------------------------------------------------
# Expired access token: retry after refresh
# ---------------------------------------------------------------------------

class TestExpiredAccessTokenRetry(unittest.TestCase):
    """When the RaaS endpoint returns 401/403 the tap refreshes and retries once."""

    @patch("tap_workday_raas.client.requests.post")
    @patch("tap_workday_raas.client.requests.get")
    def test_401_triggers_refresh_and_retry(self, mock_get, mock_post):
        auth_fail = MagicMock()
        auth_fail.ok = False
        auth_fail.status_code = 401
        auth_fail.close = MagicMock()
        body = json.dumps({"Report_Entry": [{"id": "1"}]}).encode()
        ok_resp = _make_ok_streaming_response(body)
        ok_resp.status_code = 200
        mock_get.side_effect = [auth_fail, ok_resp]
        mock_post.return_value = _make_refresh_response("refreshed-token")
        client = _make_client()
        records = list(stream_report("http://fake", client))
        mock_post.assert_called_once()
        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(
            mock_get.call_args_list[0][1]["headers"]["Authorization"],
            "Bearer test-access-token",
        )
        self.assertEqual(
            mock_get.call_args_list[1][1]["headers"]["Authorization"],
            "Bearer refreshed-token",
        )
        self.assertEqual(len(records), 1)

    @patch("tap_workday_raas.client.requests.post")
    @patch("tap_workday_raas.client.requests.get")
    def test_403_triggers_refresh_and_retry(self, mock_get, mock_post):
        auth_fail = MagicMock()
        auth_fail.ok = False
        auth_fail.status_code = 403
        auth_fail.close = MagicMock()
        ok_resp = _make_ok_streaming_response(json.dumps({"Report_Entry": []}).encode())
        ok_resp.status_code = 200
        mock_get.side_effect = [auth_fail, ok_resp]
        mock_post.return_value = _make_refresh_response()
        list(stream_report("http://fake", _make_client()))
        mock_post.assert_called_once()
        self.assertEqual(mock_get.call_count, 2)

    @patch("tap_workday_raas.client.requests.post")
    @patch("tap_workday_raas.client.requests.get")
    def test_does_not_retry_indefinitely(self, mock_get, mock_post):
        """After one retry, if auth still fails the error propagates."""
        auth_fail = MagicMock()
        auth_fail.ok = False
        auth_fail.status_code = 401
        auth_fail.close = MagicMock()
        auth_fail.content = b""
        auth_fail.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=auth_fail
        )
        auth_fail.__enter__ = MagicMock(return_value=auth_fail)
        auth_fail.__exit__ = MagicMock(return_value=False)
        mock_get.side_effect = [auth_fail, auth_fail]
        mock_post.return_value = _make_refresh_response("still-bad")
        with self.assertRaises(requests.exceptions.HTTPError):
            list(stream_report("http://fake", _make_client()))
        self.assertEqual(mock_get.call_count, 2)

    @patch("tap_workday_raas.client.requests.post")
    @patch("tap_workday_raas.client.requests.get")
    def test_200_does_not_trigger_refresh(self, mock_get, mock_post):
        body = json.dumps({"Report_Entry": [{"x": 1}]}).encode()
        mock_get.return_value = _make_ok_streaming_response(body)
        list(stream_report("http://fake", _make_client()))
        mock_post.assert_not_called()
        self.assertEqual(mock_get.call_count, 1)


# ---------------------------------------------------------------------------
# download_xsd uses OAuth Bearer auth
# ---------------------------------------------------------------------------

class TestDownloadXsdOAuth(unittest.TestCase):
    """download_xsd sends Authorization: Bearer <access_token>."""

    @patch("tap_workday_raas.client.requests.get")
    def test_bearer_token_in_xsd_request(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = "<xsd/>"
        mock_get.return_value = mock_resp
        download_xsd("http://fake/report", _make_client())
        self.assertEqual(
            mock_get.call_args[1]["headers"]["Authorization"],
            "Bearer test-access-token",
        )

    @patch("tap_workday_raas.client.requests.get")
    def test_no_basic_auth_in_xsd_request(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = "<xsd/>"
        mock_get.return_value = mock_resp
        download_xsd("http://fake/report", _make_client())
        self.assertNotIn("auth", mock_get.call_args[1])

    @patch("tap_workday_raas.client.requests.post")
    @patch("tap_workday_raas.client.requests.get")
    def test_xsd_401_triggers_refresh_and_retry(self, mock_get, mock_post):
        fail_resp = MagicMock()
        fail_resp.ok = False
        fail_resp.status_code = 401
        ok_resp = MagicMock()
        ok_resp.ok = True
        ok_resp.status_code = 200
        ok_resp.raise_for_status = MagicMock()
        ok_resp.text = "<xsd/>"
        mock_get.side_effect = [fail_resp, ok_resp]
        mock_post.return_value = _make_refresh_response("refreshed-xsd-token")
        result = download_xsd("http://fake/report", _make_client())
        self.assertEqual(result, "<xsd/>")
        mock_post.assert_called_once()
        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(
            mock_get.call_args_list[1][1]["headers"]["Authorization"],
            "Bearer refreshed-xsd-token",
        )


# ---------------------------------------------------------------------------
# Discovery uses OAuth
# ---------------------------------------------------------------------------

class TestDiscoveryUsesOAuth(unittest.TestCase):
    """discover_streams accepts auth_client and uses it for all Workday calls."""

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_discovery_passes_auth_client_to_download_xsd(self, mock_xsd, mock_enrich):
        from tap_workday_raas.discover import discover_streams

        xsd = """<?xml version="1.0"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:wd="urn:test"
            elementFormDefault="qualified" attributeFormDefault="qualified"
            targetNamespace="urn:test">
  <xsd:element name="Report_Data" type="wd:Report_DataType"/>
  <xsd:complexType name="Report_EntryType">
    <xsd:sequence>
      <xsd:element name="col_a" type="xsd:string" minOccurs="0"/>
    </xsd:sequence>
  </xsd:complexType>
  <xsd:complexType name="Report_DataType">
    <xsd:sequence>
      <xsd:element name="Report_Entry" type="wd:Report_EntryType"
                   minOccurs="0" maxOccurs="unbounded"/>
    </xsd:sequence>
  </xsd:complexType>
</xsd:schema>"""
        mock_xsd.return_value = xsd
        mock_enrich.side_effect = lambda schema, *a, **kw: schema

        auth_client = _make_client()
        discover_streams(
            _oauth_config({
                "reports": '[{"report_url": "http://fake", "report_name": "test_r"}]'
            }),
            auth_client,
        )
        self.assertIsInstance(mock_xsd.call_args[0][1], WorkdayOAuthClient)


# ---------------------------------------------------------------------------
# Secrets never logged
# ---------------------------------------------------------------------------

class TestTokensNotLogged(unittest.TestCase):
    @patch("tap_workday_raas.client.requests.post")
    def test_refresh_error_excludes_tokens(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 401
        mock_post.return_value = mock_resp
        with self.assertRaises(Exception) as ctx:
            WorkdayOAuthClient(_oauth_config())._refresh_access_token()
        err = str(ctx.exception)
        self.assertNotIn("test-client-secret", err)
        self.assertNotIn("test-refresh-token", err)

import time
import unittest
from unittest.mock import patch, MagicMock

import jwt as pyjwt
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from tap_workday_raas import _validate_auth_config
from tap_workday_raas.client import (
    WorkdayJWTBearerClient, create_auth_client, stream_report,
)
from tap_workday_raas.exceptions import WorkdayRaasAuthenticationError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_PRIVATE_KEY_PEM = _PRIVATE_KEY.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
).decode()
_PUBLIC_KEY_PEM = _PRIVATE_KEY.public_key().public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo,
).decode()


def _jwt_config(extra=None):
    """Return a minimal valid JWT Bearer config dict using hostname + tenant."""
    cfg = {
        "hostname": "test.workday.com",
        "tenant": "mytenant",
        "client_id": "test-client-id",
        "private_key": _PRIVATE_KEY_PEM,
        "isu": "test-isu",
        "reports": "[]",
    }
    if extra:
        cfg.update(extra)
    return cfg


def _make_client(**config_overrides):
    """Return a WorkdayJWTBearerClient pre-loaded with a valid in-memory token."""
    client = WorkdayJWTBearerClient(_jwt_config(config_overrides))
    client._access_token = "test-access-token"
    client._expires_at = time.monotonic() + 86400
    return client


def _make_token_response(access_token="refreshed-token", expires_in=3600):
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"access_token": access_token, "expires_in": expires_in}
    return mock_resp


def _make_ok_streaming_response(body_bytes):
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.status_code = 200
    mock_resp.iter_content.return_value = [body_bytes]
    mock_resp.raise_for_status = MagicMock()
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


# ---------------------------------------------------------------------------
# Token endpoint derivation (shared convention with WorkdayOAuthClient)
# ---------------------------------------------------------------------------

class TestJWTBearerTokenEndpointDerivation(unittest.TestCase):
    def test_derives_endpoint_from_hostname_and_tenant(self):
        client = WorkdayJWTBearerClient(_jwt_config())
        self.assertEqual(
            client._token_endpoint,
            "https://test.workday.com/ccx/oauth2/mytenant/token",
        )

    def test_explicit_token_endpoint_overrides_derivation(self):
        client = WorkdayJWTBearerClient(
            _jwt_config({"token_endpoint": "https://custom.example.com/token"})
        )
        self.assertEqual(client._token_endpoint, "https://custom.example.com/token")


# ---------------------------------------------------------------------------
# Assertion construction (RS256-signed JWT)
# ---------------------------------------------------------------------------

class TestBuildAssertion(unittest.TestCase):
    def test_assertion_is_signed_with_rs256_and_verifiable(self):
        client = WorkdayJWTBearerClient(_jwt_config())
        assertion = client._build_assertion()
        decoded = pyjwt.decode(
            assertion, _PUBLIC_KEY_PEM, algorithms=["RS256"], audience="wd",
        )
        self.assertEqual(decoded["iss"], "test-client-id")
        self.assertEqual(decoded["sub"], "test-isu")
        self.assertEqual(decoded["aud"], "wd")
        self.assertIn("exp", decoded)

    def test_isu_used_as_subject(self):
        client = WorkdayJWTBearerClient(_jwt_config({"isu": "other-isu-user"}))
        assertion = client._build_assertion()
        decoded = pyjwt.decode(
            assertion, _PUBLIC_KEY_PEM, algorithms=["RS256"], audience="wd",
        )
        self.assertEqual(decoded["sub"], "other-isu-user")

    def test_custom_assertion_ttl_applied(self):
        client = WorkdayJWTBearerClient(_jwt_config({"jwt_assertion_ttl_secs": 60}))
        before = int(time.time())
        assertion = client._build_assertion()
        decoded = pyjwt.decode(
            assertion, _PUBLIC_KEY_PEM, algorithms=["RS256"], audience="wd",
        )
        self.assertGreaterEqual(decoded["exp"], before + 60)
        self.assertLessEqual(decoded["exp"], before + 61)

    def test_default_assertion_ttl_is_300_seconds(self):
        client = WorkdayJWTBearerClient(_jwt_config())
        before = int(time.time())
        assertion = client._build_assertion()
        decoded = pyjwt.decode(
            assertion, _PUBLIC_KEY_PEM, algorithms=["RS256"], audience="wd",
        )
        self.assertGreaterEqual(decoded["exp"], before + 300)
        self.assertLessEqual(decoded["exp"], before + 301)

    def test_invalid_private_key_raises_authentication_error(self):
        client = WorkdayJWTBearerClient(_jwt_config({"private_key": "not-a-valid-key"}))
        with self.assertRaises(WorkdayRaasAuthenticationError):
            client._build_assertion()


# ---------------------------------------------------------------------------
# _refresh_access_token - JWT Bearer grant request
# ---------------------------------------------------------------------------

class TestJWTBearerRefreshAccessToken(unittest.TestCase):
    @patch("tap_workday_raas.client.requests.post")
    def test_sends_jwt_bearer_grant_in_body(self, mock_post):
        mock_post.return_value = _make_token_response()
        client = WorkdayJWTBearerClient(_jwt_config())
        client._refresh_access_token()
        call_data = mock_post.call_args[1]["data"]
        self.assertEqual(
            call_data["grant_type"], "urn:ietf:params:oauth:grant-type:jwt-bearer"
        )
        self.assertIn("assertion", call_data)

    @patch("tap_workday_raas.client.requests.post")
    def test_no_http_basic_auth_used(self, mock_post):
        """JWT Bearer grant must not send client_id/secret via HTTP Basic auth."""
        mock_post.return_value = _make_token_response()
        client = WorkdayJWTBearerClient(_jwt_config())
        client._refresh_access_token()
        self.assertNotIn("auth", mock_post.call_args[1])

    @patch("tap_workday_raas.client.requests.post")
    def test_posts_to_derived_token_endpoint(self, mock_post):
        mock_post.return_value = _make_token_response()
        client = WorkdayJWTBearerClient(_jwt_config())
        client._refresh_access_token()
        self.assertEqual(
            mock_post.call_args[0][0],
            "https://test.workday.com/ccx/oauth2/mytenant/token",
        )

    @patch("tap_workday_raas.client.requests.post")
    def test_updates_access_token_and_expiry(self, mock_post):
        mock_post.return_value = _make_token_response("brand-new-token", expires_in=1800)
        client = WorkdayJWTBearerClient(_jwt_config())
        before = time.monotonic()
        client._refresh_access_token()
        self.assertEqual(client._access_token, "brand-new-token")
        self.assertGreater(client._expires_at, before + 1790)

    @patch("tap_workday_raas.client.requests.post")
    def test_401_raises_clear_error(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 401
        mock_post.return_value = mock_resp
        with self.assertRaises(WorkdayRaasAuthenticationError) as ctx:
            WorkdayJWTBearerClient(_jwt_config())._refresh_access_token()
        self.assertIn("401", str(ctx.exception))

    @patch("tap_workday_raas.client.requests.post")
    def test_failure_does_not_expose_private_key(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 400
        mock_resp.json.side_effect = ValueError()
        mock_post.return_value = mock_resp
        with self.assertRaises(WorkdayRaasAuthenticationError) as ctx:
            WorkdayJWTBearerClient(_jwt_config())._refresh_access_token()
        self.assertNotIn(_PRIVATE_KEY_PEM, str(ctx.exception))

    @patch("tap_workday_raas.client.requests.post")
    def test_network_error_raises(self, mock_post):
        mock_post.side_effect = requests.RequestException("connection refused")
        with self.assertRaises(WorkdayRaasAuthenticationError) as ctx:
            WorkdayJWTBearerClient(_jwt_config())._refresh_access_token()
        self.assertIn("network error", str(ctx.exception))

    @patch("tap_workday_raas.client.requests.post")
    def test_missing_access_token_in_response_raises(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"token_type": "Bearer"}
        mock_post.return_value = mock_resp
        with self.assertRaises(WorkdayRaasAuthenticationError) as ctx:
            WorkdayJWTBearerClient(_jwt_config())._refresh_access_token()
        self.assertIn("access_token", str(ctx.exception))

    @patch("tap_workday_raas.client.requests.post")
    def test_non_json_token_response_raises(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.side_effect = ValueError("No JSON object could be decoded")
        mock_post.return_value = mock_resp
        with self.assertRaises(WorkdayRaasAuthenticationError) as ctx:
            WorkdayJWTBearerClient(_jwt_config())._refresh_access_token()
        self.assertIn("non-JSON", str(ctx.exception))


# ---------------------------------------------------------------------------
# get_access_token - proactive expiry handling (same semantics as OAuth client)
# ---------------------------------------------------------------------------

class TestJWTBearerGetAccessToken(unittest.TestCase):
    def test_returns_cached_token_when_valid(self):
        self.assertEqual(_make_client().get_access_token(), "test-access-token")

    @patch("tap_workday_raas.client.requests.post")
    def test_refreshes_when_token_expired(self, mock_post):
        mock_post.return_value = _make_token_response("fresh-token")
        client = WorkdayJWTBearerClient(_jwt_config())
        client._access_token = "stale-token"
        client._expires_at = time.monotonic() - 100
        self.assertEqual(client.get_access_token(), "fresh-token")
        mock_post.assert_called_once()

    @patch("tap_workday_raas.client.requests.post")
    def test_does_not_refresh_when_valid(self, mock_post):
        _make_client().get_access_token()
        mock_post.assert_not_called()


# ---------------------------------------------------------------------------
# __enter__/__exit__ context manager behavior
# ---------------------------------------------------------------------------

class TestJWTBearerContextManager(unittest.TestCase):
    @patch("tap_workday_raas.client.requests.post")
    def test_enter_fetches_token(self, mock_post):
        mock_post.return_value = _make_token_response("entry-token")
        with WorkdayJWTBearerClient(_jwt_config()) as client:
            self.assertEqual(client._access_token, "entry-token")

    def test_exit_does_not_raise(self):
        _make_client().__exit__(None, None, None)


# ---------------------------------------------------------------------------
# Bearer token used in requests, with 401/403 retry
# ---------------------------------------------------------------------------

class TestJWTBearerUsedInRequests(unittest.TestCase):
    @patch("tap_workday_raas.client.requests.get")
    def test_bearer_token_present_in_stream_report(self, mock_get):
        body = b'{"Report_Entry": [{"col": "val"}]}'
        mock_get.return_value = _make_ok_streaming_response(body)
        list(stream_report("http://fake", _make_client()))
        headers = mock_get.call_args[1].get("headers", {})
        self.assertEqual(headers.get("Authorization"), "Bearer test-access-token")

    @patch("tap_workday_raas.client.requests.post")
    @patch("tap_workday_raas.client.requests.get")
    def test_401_triggers_refresh_and_retry(self, mock_get, mock_post):
        auth_fail = MagicMock()
        auth_fail.ok = False
        auth_fail.status_code = 401
        auth_fail.close = MagicMock()
        ok_resp = _make_ok_streaming_response(b'{"Report_Entry": []}')
        mock_get.side_effect = [auth_fail, ok_resp]
        mock_post.return_value = _make_token_response("refreshed-token")
        list(stream_report("http://fake", _make_client()))
        mock_post.assert_called_once()
        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(
            mock_get.call_args_list[1][1]["headers"]["Authorization"],
            "Bearer refreshed-token",
        )


# ---------------------------------------------------------------------------
# create_auth_client factory - jwt_bearer mode
# ---------------------------------------------------------------------------

class TestCreateAuthClientJWTBearer(unittest.TestCase):
    def test_returns_jwt_bearer_client_for_jwt_bearer_auth_method(self):
        cfg = {
            "auth_method": "jwt_bearer",
            "hostname": "test.workday.com",
            "tenant": "mytenant",
            "client_id": "cid",
            "private_key": _PRIVATE_KEY_PEM,
            "isu": "test-isu",
        }
        client = create_auth_client(cfg)
        self.assertIsInstance(client, WorkdayJWTBearerClient)

    def test_jwt_bearer_missing_isu_raises(self):
        cfg = {
            "auth_method": "jwt_bearer",
            "hostname": "test.workday.com",
            "tenant": "mytenant",
            "client_id": "cid",
            "private_key": _PRIVATE_KEY_PEM,
        }
        with self.assertRaises(WorkdayRaasAuthenticationError):
            create_auth_client(cfg)

    def test_jwt_bearer_missing_keys_raises(self):
        cfg = {
            "auth_method": "jwt_bearer",
            "hostname": "test.workday.com",
            "client_id": "cid",
        }
        with self.assertRaises(WorkdayRaasAuthenticationError):
            create_auth_client(cfg)

    def test_jwt_bearer_not_inferred_without_explicit_auth_method(self):
        """A config with jwt-bearer-shaped keys but no auth_method must not
        silently resolve to a JWT Bearer client (ambiguous with legacy
        inference); it should raise since it lacks refresh_token/username."""
        cfg = {
            "hostname": "test.workday.com",
            "tenant": "mytenant",
            "client_id": "cid",
            "private_key": _PRIVATE_KEY_PEM,
            "isu": "test-isu",
        }
        with self.assertRaises(WorkdayRaasAuthenticationError):
            create_auth_client(cfg)

    def test_passes_config_path_to_jwt_bearer_client(self):
        cfg = {
            "auth_method": "jwt_bearer",
            "hostname": "test.workday.com",
            "tenant": "mytenant",
            "client_id": "cid",
            "private_key": _PRIVATE_KEY_PEM,
            "isu": "test-isu",
        }
        client = create_auth_client(cfg, config_path="/tmp/config.json")
        self.assertEqual(client._config_path, "/tmp/config.json")


# ---------------------------------------------------------------------------
# _validate_auth_config - jwt_bearer mode
# ---------------------------------------------------------------------------

class TestValidateAuthConfigJWTBearer(unittest.TestCase):
    def test_validate_jwt_bearer_mode(self):
        cfg = {
            "auth_method": "jwt_bearer",
            "hostname": "test.workday.com",
            "tenant": "mytenant",
            "client_id": "cid",
            "private_key": _PRIVATE_KEY_PEM,
            "isu": "test-isu",
            "reports": "[]",
        }
        _validate_auth_config(cfg)  # should not raise

    def test_validate_jwt_bearer_mode_missing_private_key_fails(self):
        cfg = {
            "auth_method": "jwt_bearer",
            "hostname": "test.workday.com",
            "tenant": "mytenant",
            "client_id": "cid",
            "isu": "test-isu",
            "reports": "[]",
        }
        with self.assertRaises(WorkdayRaasAuthenticationError):
            _validate_auth_config(cfg)

    def test_validate_jwt_bearer_mode_missing_isu_fails(self):
        cfg = {
            "auth_method": "jwt_bearer",
            "hostname": "test.workday.com",
            "tenant": "mytenant",
            "client_id": "cid",
            "private_key": _PRIVATE_KEY_PEM,
            "reports": "[]",
        }
        with self.assertRaises(WorkdayRaasAuthenticationError):
            _validate_auth_config(cfg)


if __name__ == "__main__":
    unittest.main()

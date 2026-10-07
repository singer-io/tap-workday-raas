# tap-workday-raas

[![PyPI version](https://badge.fury.io/py/tap-mysql.svg)](https://badge.fury.io/py/tap-workday-raas)
[![CircleCI Build Status](https://circleci.com/gh/singer-io/tap-workday-raas.png)](https://circleci.com/gh/singer-io/tap-workday-raas.png)


[Singer](https://www.singer.io/) tap that extracts data from a [Workday](https://www.workday.com/) report and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

```bash
$ mkvirtualenv -p python3 tap-workday-raas
$ pip install tap-workday-raas
$ tap-workday-raas --config config.json --discover
$ tap-workday-raas --config config.json --properties properties.json --state state.json
```

# Quickstart

## Install

   Clone this repository, and then install using setup.py. We recommend using a virtualenv:

   ```bash
   $ virtualenv -p python3 venv
   $ source venv/bin/activate
   $ pip install -e .
   ```
## Create Config

   Create your tap's `config.json` file. The tap supports three authentication
   modes, selected via the `auth_method` field.

   The `reports` field is required for all modes:

   - `reports` – A JSON string containing a list of objects, each with a
     `report_name` and `report_url`. `report_name` is used as the Singer
     stream name.

   ---

   ### Mode 1 — OAuth 2.0 (`auth_method: authorization_code`)

   Use this mode when you have a Workday OAuth 2.0 API client configured with
   the **Authorization Code** grant. You must complete the authorization flow
   to obtain a `refresh_token` before running the tap.

   Required fields:

   | Field | Description |
   |-------|-------------|
   | `auth_method` | Must be `"authorization_code"` |
   | `hostname` | Workday hostname |
   | `tenant` | Workday tenant name |
   | `client_id` | OAuth client ID registered in Workday |
   | `client_secret` | OAuth client secret |
   | `refresh_token` | Valid refresh token from the authorization flow |

   ```json
   {
       "auth_method": "authorization_code",
       "hostname": "<WORKDAY_HOSTNAME>",
       "tenant": "<TENANT>",
       "client_id": "<CLIENT_ID>",
       "client_secret": "<CLIENT_SECRET>",
       "refresh_token": "<REFRESH_TOKEN>",
       "reports": "[{\"report_name\": \"my_report\", \"report_url\": \"https://...\"}]"
   }
   ```

   #### Token refresh behaviour

   The tap exchanges `refresh_token` for an access token via HTTP Basic auth
   (`client_id`:`client_secret`) at:

   ```
   https://<hostname>/ccx/oauth2/<tenant>/token
   ```

   The access token is sent as a Bearer token on every RaaS and XSD request.
   If Workday rejects a request with HTTP 401 or 403, the tap refreshes the
   token automatically and retries once.

   Workday issues a new `refresh_token` with each token exchange (rotating
   tokens). The tap persists the updated token back to `config.json`
   immediately so subsequent tap processes use the current token.

   ---

   ### Mode 2 — OAuth 2.0 JWT Bearer Token (`auth_method: jwt_bearer`)

   Use this mode when you have a Workday Integration System User (ISU) or API
   client configured for the **JWT Bearer Token** grant (RFC 7523). No
   refresh token or interactive authorization step is required — the tap
   signs a short-lived JWT assertion with your private key for every token
   request. The matching public key/certificate must be registered on the
   Workday API client/ISU.

   Required fields:

   | Field | Description |
   |-------|-------------|
   | `auth_method` | Must be `"jwt_bearer"` |
   | `jwt_hostname` | Workday hostname |
   | `jwt_tenant` | Workday tenant name |
   | `jwt_client_id` | OAuth client ID registered in Workday |
   | `private_key` | PEM-encoded RSA private key used to sign the JWT assertion |
   | `isu` | Integration System User - used as the JWT `sub` claim |

   Optional fields:

   | Field | Description |
   |-------|-------------|
   | `jwt_assertion_ttl_secs` | JWT assertion lifetime in seconds (default `300`) |
   | `token_endpoint` | Override the derived token endpoint |

   ```json
   {
       "auth_method": "jwt_bearer",
       "jwt_hostname": "<WORKDAY_HOSTNAME>",
       "jwt_tenant": "<TENANT>",
       "jwt_client_id": "<CLIENT_ID>",
       "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
       "isu": "<ISU_USERNAME>",
       "reports": "[{\"report_name\": \"my_report\", \"report_url\": \"https://...\"}]"
   }
   ```

   #### Token request behaviour

   The tap signs an RS256 JWT assertion (`iss` = `jwt_client_id`, `sub` = `isu`,
   `aud` = the fixed value `"wd"`, short `exp`) and exchanges it at:

   ```
   https://<jwt_hostname>/ccx/oauth2/<jwt_tenant>/token
   ```

   using `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer`. The
   resulting access token is sent as a Bearer token on every RaaS and XSD
   request, with the same automatic refresh-and-retry-once behavior on HTTP
   401/403 as the Authorization Code mode.

   ---

   ### Mode 3 — Basic Auth (`auth_method: client_credentials`)

   Use this mode when authenticating with a Workday username and password.

   Required fields:

   | Field | Description |
   |-------|-------------|
   | `auth_method` | Must be `"client_credentials"` |
   | `username` | Workday username |
   | `password` | Workday password |

   ```json
   {
       "auth_method": "client_credentials",
       "username": "<USERNAME>",
       "password": "<PASSWORD>",
       "reports": "[{\"report_name\": \"my_report\", \"report_url\": \"https://...\"}]"
   }
   ```

   > **Note:** Basic Auth credentials are static — no token exchange or refresh
   > occurs. Every request is authenticated directly with `username`/`password`.

## Run Discovery

To run discovery mode, execute the tap with the config file.

```
> tap-workday-raas --config config.json --discover > properties.json
```

## Sync Data

To sync data, select fields in the `properties.json` output and run the tap.

```
> tap-workday-raas --config config.json --properties properties.json [--state state.json]
```

Copyright &copy; 2020 Stitch

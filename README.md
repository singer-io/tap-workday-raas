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

   Create your tap's `config.json` file. The tap supports two authentication
   modes — **OAuth 2.0** (recommended for new connections) and **HTTP Basic
   Auth** (supported for existing connections).

   The only field required by both modes is:

   - `reports` – A JSON string containing a list of objects, each with a
     `report_name` and `report_url`.  `report_name` is used as the Singer
     stream name.

   ---

   ### OAuth 2.0 (recommended)

   You must complete the OAuth authorization flow externally (e.g. using the
   Workday authorization endpoint) to obtain a refresh token before running
   the tap.

   Required fields:

   - `tenant` – The name of the Workday tenant.
   - `hostname` – The Workday hostname used for API requests.
   - `client_id` – The OAuth client ID registered in Workday.
   - `client_secret` – The OAuth client secret registered in Workday.
   - `refresh_token` – A valid OAuth refresh token obtained from the authorization flow.

   ```json
   {
       "tenant": "<TENANT>",
       "hostname": "<WORKDAY_HOSTNAME>",
       "client_id": "<CLIENT_ID>",
       "client_secret": "<CLIENT_SECRET>",
       "refresh_token": "<REFRESH_TOKEN>",
       "reports": "[{\"report_name\": \"my_report\", \"report_url\": \"https://...\"}]"
   }
   ```

   #### Token refresh behaviour

   The tap exchanges the `refresh_token` for an access token using HTTP Basic
   auth (`client_id`:`client_secret`) at the Workday token endpoint
   (`https://<hostname>/ccx/oauth2/<tenant>/token`).  The access token is sent
   as a Bearer token on every Workday RaaS and XSD request.

   If Workday rejects a request with HTTP 401 or 403, the tap refreshes the
   access token automatically and retries once.

   Workday may return a new `refresh_token` when an access token is refreshed.
   When this occurs, the tap updates the in-memory config and persists the new
   token back to `config.json` so subsequent tap processes use the current
   refresh token.

   ---

   ### HTTP Basic Auth (existing connections)

   Connections created before OAuth support was added continue to work using
   `username` and `password`.

   Required fields:

   - `username` – The Workday username.
   - `password` – The Workday password.

   ```json
   {
       "username": "<USERNAME>",
       "password": "<PASSWORD>",
       "reports": "[{\"report_name\": \"my_report\", \"report_url\": \"https://...\"}]"
   }
   ```

   > **Note:** Basic Auth credentials are static — no token refresh occurs.

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

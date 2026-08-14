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

   Create your tap's `config.json` file.  The tap authenticates to Workday using
   OAuth 2.0.  You must complete the OAuth authorization flow externally (e.g.
   using the Workday authorization endpoint) to obtain a refresh token before
   running the tap.

   Required fields:

   - `tenant` – The name of the Workday tenant.
   - `hostname` – The Workday hostname used for API requests.
   - `client_id` – The OAuth client ID registered in Workday.
   - `client_secret` – The OAuth client secret registered in Workday.
   - `refresh_token` – A valid OAuth refresh token obtained from the authorization flow.
   - `reports` – A JSON string containing a list of objects, each with a `report_name`
     and `report_url`.  `report_name` is used as the Singer stream name.

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

   ### Token refresh behaviour

   The tap uses the configured `refresh_token` to obtain an access token from the Workday OAuth token endpoint. The access token is then used as a Bearer token for Workday RaaS and XSD requests.

   If Workday rejects a request with HTTP 401 or 403, the tap automatically refreshes the access token and retries the request once.

   Workday may return a new `refresh_token` when an access token is refreshed. When this occurs, the tap updates the in-memory refresh token and persists the new refresh token to `config.json` so that subsequent tap processes continue to use the current refresh token.

   > **Note:** `username` and `password` are no longer required or supported.
   > Basic Authentication has been removed.

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

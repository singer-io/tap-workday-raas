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

   Create your tap's `config.json` file.  The tap config file for this tap should include these entries:

   - `username` - The username of the workday account with access to the reports to extract
   - `password` - The password of the workday account with access to the reports to extract
   - `reports` -  A JSON string containing a list of objects containing the `report_name` and `report_url`. `report_name` is the name of the stream for the report, and the `report_url` is the URL to the Workday XML REST link for the report you wish to extract.

   ```json
   {
       "username": "<username>",
       "password": "<password>",
       "reports": "[{\"report_name\": \"abitrary_name\", \"report_url\": \"https://...\"}, ...]"
   }
   ```

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

import requests
import ijson.backends.yajl2_c as ijson
import ijson as ijson_core
from tap_workday_raas.symon_exception import SymonException


def stream_report(report_url, user, password):
    # Force the format query param to be set to format=json

    # Split query params off
    url_breakdown = report_url.split("?")

    # Gather all params that are not format
    if len(url_breakdown) == 1:
        params = []
    else:
        params = [x for x in url_breakdown[1].split(
            "&") if not x.startswith("format=")]

    # Add the format param
    params.append("format=json")
    param_string = "&".join(params)

    # Put the url back together
    corrected_url = url_breakdown[0] + "?" + param_string

    # Get the data
    with requests.get(corrected_url, auth=(user, password), stream=True) as resp:
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if '401 Client Error: Unauthorized for url' in str(e):
                raise SymonException('Invalid username or password. Please update your connection credentials and try again.',
                                     'workday.InvalidUsernameOrPassword')
            raise

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

        found_key = False
        for chunk in resp.iter_content(chunk_size=512):
            if report_entry_key in chunk:
                found_key = True
            coro.send(chunk)
            for rec in records:
                yield rec
            del records[:]

        if not found_key:
            raise Exception(
                "Did not see '{}' key in response. Report does not conform to expected schema, failing.".format(report_entry_key))

        coro.close()


def download_xsd(report_url, user, password):
    if "?" in report_url:
        xsds_url = report_url.split("?")[0] + "?xsds"
    else:
        xsds_url = report_url + "?xsds"
    response = requests.get(xsds_url, auth=(user, password))

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if '500 Server Error: Internal Server Error for url' in str(e):
            raise SymonException('Invalid report URL',
                                 'workdayRaaS.WorkdayRaaSInvalidReportURL')
        raise

    return response.text

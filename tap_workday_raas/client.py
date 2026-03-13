import requests
import ijson.backends.yajl2_c as ijson
import ijson as ijson_core
import singer

LOGGER = singer.get_logger()


def stream_report(report_url, user, password):
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

    # Get the data
    with requests.get(corrected_url, auth=(user, password), stream=True) as resp:
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
        found_report_data = False
        found_report_entry = False

        for chunk in resp.iter_content(chunk_size=512):
            coro.send(chunk)
            key_coro.send(chunk)

            # Scan parser events returned so far for the keys we care about
            for prefix, event, _value in key_events:
                if prefix == "" and event == "map_key" and _value == "Report_Data":
                    found_report_data = True
                elif prefix == "Report_Data" and event == "map_key" and _value == report_entry_key.decode("utf-8"):
                    found_report_entry = True
            del key_events[:]

            for rec in records:
                yield rec
            del records[:]

        if not found_report_entry:
            if found_report_data:
                # Known empty-response shape: {"Report_Data": {}} with no
                # Report_Entry key.  This is the standard Workday zero-row
                # response – log a warning and continue.
                LOGGER.warning(
                    "Did not see '%s' key in response. "
                    "Report returned 0 rows (empty result set).",
                    report_entry_key.decode("utf-8"),
                )
            else:
                # Unexpected payload – neither Report_Data nor Report_Entry
                # found.  This likely indicates a schema change or API error.
                raise Exception(
                    "Did not see 'Report_Data' or '{}' key in response. "
                    "Report does not conform to expected schema, failing."
                    .format(report_entry_key.decode("utf-8"))
                )

        coro.close()
        key_coro.close()


def download_xsd(report_url, user, password):
    if "?" in report_url:
        xsds_url = report_url.split("?")[0] + "?xsds"
    else:
        xsds_url = report_url + "?xsds"
    response = requests.get(xsds_url, auth=(user, password))
    response.raise_for_status()

    return response.text

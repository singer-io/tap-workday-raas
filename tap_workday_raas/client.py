import requests

def stream_report(report_url, user, password):
    # Force the format query param to be set to format=json

    # Split query params off
    url_breakdown = report_url.split('?')

    # Gather all params that are not format
    if len(url_breakdown) == 1:
        params = []
    else:
        params = [x for x in url_breakdown[1].split('&') if not x.startswith('format')]

    # Add the format param
    params.append('format=json')
    param_string = '&'.join(params)

    # Put the url back together
    corrected_url = url_breakdown[0] + '?' + param_string

    # Get the data
    with requests.get(corrected_url, auth=(user, password), stream=True) as resp:
        resp.raise_for_status()
        report = resp.json() # TODO Check that this is streaming. I don't think it is
        for record in report['Report_Entry']:
            yield record

def download_xsd(report_url, user, password):
    if '?' in report_url:
        xsds_url = report_url.split('?')[0] + '?xsds'
    else:
        xsds_url = report_url + '?xsds'
    response = requests.get(xsds_url, auth=(user, password))
    response.raise_for_status()

    return response.text

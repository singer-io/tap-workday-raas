import requests
from xml.etree import ElementTree
import io

def stream_report(report_url, user, password):
    with requests.get(report_url, auth=(user, password), stream=True) as resp:
        with io.BytesIO(resp.content) as stream:
            try:
                context = ElementTree.iterparse(stream, events=("start", "end"))
                for event, elem in context:
                    yield (event, elem)
            except ElementTree.ParseError as e:
                raise Exception("Report URL {} does not parse as XML. Please ensure the integration is configured with the correct URL".format(report_url))

def download_xsd(report_url, user, password):
    if '?' in report_url:
        xsds_url = report_url.split('?')[0] + '?xsds'
    else:
        xsds_url = report_url + '?xsds'
    response = requests.get(xsds_url, auth=(user, password))
    response.raise_for_status()

    return response.text

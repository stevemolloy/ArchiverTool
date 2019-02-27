from argparse import ArgumentParser
import requests
import json
import asyncio
from functools import partial

BASEURL = 'http://control.maxiv.lu.se/general/archiving/'
SEARCHURL = BASEURL + 'search'
QUERYURL = BASEURL + 'query'
CONTROLURL = "g-v-csdb-0.maxiv.lu.se:10000"

def makesearchpayload(searchterm):
    return {
            'cs': CONTROLURL,
            'target': searchterm
            }

def makequerypayload(signal, start, end):
    return {
            'targets': [{'target': signal, 'cs': CONTROLURL,}],
            'range': {'from': start, 'to': end,}
            }

def parse_response(resp):
    if not resp.status_code == 200:
        data = {
                'target': 'Attribute not found in HDB++',
                'datapoints': [],
                }
    else:
        data = json.loads(resp.text)[0]
    output = []
    output.append('# ' + data['target'])
    output.append('# Time, Value')
    for vals in data['datapoints']:
        output.append('{}, {}'.format(vals[1], vals[0]))
    return '\n'.join(output)

@asyncio.coroutine
def do_request(args):
    loop = asyncio.get_event_loop()
    futures, responses = [], []
    for sig in args.signals:
        payload = makequerypayload(sig, args.start, args.end)
        futures.append(loop.run_in_executor(
                None,
                partial(requests.post, url=QUERYURL, json=payload),
                ))
    for fut in futures:
        resp = yield from fut
        responses.append(resp)
    return responses

if __name__=="__main__":
    parser = ArgumentParser(description='Get data from HDB++ archiver')
    parser.add_argument(
            'signals',
            type=str,
            nargs='+',
            help='Signal(s) to acquire',
            )
    parser.add_argument(
            '-s', '--start',
            type=str,
            required=True,
            help='Start of time-range',
            )
    parser.add_argument(
            '-e', '--end',
            type=str,
            required=True,
            help='End of time-range'
            )

    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    response = loop.run_until_complete(do_request(args))
    for resp in response:
        print(parse_response(resp))


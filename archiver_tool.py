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
    return json.loads(resp.text)

@asyncio.coroutine
def do_request(args):
    loop = asyncio.get_event_loop()
    payload = makequerypayload(args.query, args.start, args.end)
    future0 = loop.run_in_executor(
            None,
            partial(requests.post, url=QUERYURL, json=payload),
            )
    future1 = loop.run_in_executor(
            None,
            partial(requests.post, url=QUERYURL, json=payload),
            )
    response0 = yield from future0
    response1 = yield from future1
    return [response0, response1]

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
    print([parse_response(resp) for resp in response])

    # r = requests.post(
    #         url=QUERYURL,
    #         json=makequerypayload(args.query, args.start, args.end)
    #         )
    # print(json.loads(r.text))

    if args.searchterm is not None:
        r = requests.post(
                url=SEARCHURL,
                json=makesearchpayload(args.searchterm),
                )
        reply = json.loads(r.text)
        for val in reply:
            print(val)


from datetime import datetime
from pytz import timezone
from argparse import ArgumentParser, FileType
import requests
import json
import asyncio
from functools import partial

BASEURL = 'http://control.maxiv.lu.se/general/archiving/'
SEARCHURL = BASEURL + 'search'
QUERYURL = BASEURL + 'query'
CONTROLURL = "g-v-csdb-0.maxiv.lu.se:10000"
UTC = timezone('UTC')

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
        dt = datetime.fromtimestamp(vals[1] / 1000, tz=UTC)
        timestamp = dt.strftime("%Y-%m-%d_%H:%M:%S.%f")
        output.append('{} {}'.format(timestamp, vals[0]))
    return '\n'.join(output) + '\n'

@asyncio.coroutine
def do_request(start, end, signals):
    loop = asyncio.get_event_loop()
    futures, responses = [], []
    for sig in signals:
        payload = makequerypayload(sig, start, end)
        futures.append(loop.run_in_executor(
                None,
                partial(requests.post, url=QUERYURL, json=payload),
                ))
    for fut in futures:
        resp = yield from fut
        responses.append(resp)
    return responses

if __name__=="__main__":
    parser = ArgumentParser(
            description='Get data from HDB++ archiver',
            epilog='''
            When specifying signals, note that the wildcard
            character, '*', will not work as in a POSIX
            shell, but will be interpreted as part of the regex.  Where
            you would use '*' at a POSIX shell, you probably want '.*'.
            On ZSH, the '.*' will give an error -- zsh: no matches found.
            This is due to old globbing rules in that shell, and you need
            to escape the wildcard character to make it work -- '.\*'
            ''')
    parser.add_argument(
            'signal', type=str, nargs='+',
            help='''
            Signal(s) to acquire. These are all interpreted as regex's
            beginning and ending with '.*'.
            ''',
           )
    parser.add_argument(
            '-f', '--file', type=FileType('a'),
            help='''
            Name of file in which to save the data. Use of this option
            suppresses standard output.
            '''
            )
    required = parser.add_argument_group('required arguments')
    required.add_argument(
            '-s', '--start', type=str, required=True,
            help='Start of time-range',
            )
    required.add_argument(
            '-e', '--end', type=str, required=True,
            help='End of time-range'
            )

    args = parser.parse_args()

    attributes = []
    for sig in args.signal:
        search_payload = {
                'target': sig,
                'cs': CONTROLURL,
                }
        search_resp = requests.post(SEARCHURL, json=search_payload)
        attributes += json.loads(search_resp.text)

    loop = asyncio.get_event_loop()
    response = loop.run_until_complete(
            do_request(args.start, args.end, attributes)
            )
    if args.file:
        for resp in response:
            args.file.write(parse_response(resp))
    else:
        for resp in response:
            print(parse_response(resp))


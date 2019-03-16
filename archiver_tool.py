from datetime import datetime
from math import log10, ceil
from pytz import timezone
from argparse import ArgumentParser, FileType, ArgumentTypeError
import requests
import json
import asyncio
from functools import partial
import re

BASEURL = 'http://control.maxiv.lu.se/general/archiving/'
SEARCHURL = BASEURL + 'search'
QUERYURL = BASEURL + 'query'
CONTROLURL = "g-v-csdb-0.maxiv.lu.se:10000"
UTC = timezone('UTC')
CET = timezone('CET')

def makesearchpayload(searchterm):
    return {
            'target': searchterm,
            'cs': CONTROLURL,
            }

def makequerypayload(signal, start, end, interval):
    start_naive = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
    end_naive = datetime.strptime(end, "%Y-%m-%dT%H:%M:%S")
    start_cet = CET.localize(start_naive)
    end_cet = CET.localize(end_naive)
    start_utc = start_cet.astimezone(UTC)
    end_utc = end_cet.astimezone(UTC)
    return {
            'targets': [{'target': signal, 'cs': CONTROLURL,}],
            'range': {
                'from': start_utc.isoformat(),
                'to': end_utc.isoformat(),
                },
            'interval': interval
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
    target_str = data['target'].replace(CONTROLURL+'/', CONTROLURL+'//')
    datetime_str = datetime.isoformat(datetime.now(), sep=':')
    output.append('"# DATASET= tango://' + target_str + '"')
    output.append('"# SNAPSHOT_TIME= ' + datetime_str + '"')
    for vals in data['datapoints']:
        dt = datetime.fromtimestamp(vals[1] / 1000) #, tz=CET)
        timestamp = dt.strftime("%Y-%m-%d_%H:%M:%S.%f")
        output.append('{} {}'.format(timestamp, vals[0]))
    return '\n'.join(output) + '\n'

def get_attributes(search_strs):
    attributes = []
    for sig in search_strs:
        search_payload = makesearchpayload(sig)
        search_resp = requests.post(SEARCHURL, json=search_payload)
        attributes += json.loads(search_resp.text)
    return attributes

@asyncio.coroutine
def do_request(start, end, signals, interval):
    loop = asyncio.get_event_loop()
    futures, responses = [], []
    for sig in signals:
        payload = makequerypayload(sig, start, end, interval)
        futures.append(loop.run_in_executor(
                None,
                partial(requests.post, url=QUERYURL, json=payload),
                ))
    for fut in futures:
        resp = yield from fut
        responses.append(resp)
    return responses

if __name__=="__main__":
    def interval_value(val):
        if not re.match('\d+\.*\d*[smh]$', str(val)):
            raise ArgumentTypeError(
                    'INTERVAL must be a number followed by s, m, h, or d'
                    )
        return val

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
            '-f', '--file', type=str,
            help='''
            Root name of file(s) in which to save the data. In the case of
            aquisition of a single attribute, a single file will be created
            with the name FILE.dat. In the case of multiple attribute
            aquisition, each attribute will have the name FILE001.dat,
            FILE002.dat, etc.
            If the file(s) already exist(s), it/they will be overwritten, so
            use with care. Use of this option suppresses standard output.
            '''
            )
    parser.add_argument(
            '-i', '--interval', type=interval_value, default='0.1s',
            help='''
            Force a sampling interval for the data. By default this will be
            0.1s; i.e., as dense as possible.
            This should be written in the form of a number and a time-unit;
            e.g., "1s" to sample every second, "2m" to sample every two
            minutes, "1h" to sample every hour, etc.
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

    loop = asyncio.get_event_loop()

    attributes = get_attributes(args.signal)
    response = loop.run_until_complete(
            do_request(args.start, args.end, attributes, args.interval)
            )
    if args.file:
        numfiles = len(attributes)
        numdigits = ceil(log10(numfiles + 1))
        for i, resp in enumerate(response):
            filename = args.file + str(i+1).zfill(numdigits) + '.dat'
            with open(filename, 'w') as f:
                f.write(parse_response(resp))
    else:
        for resp in response:
            print(parse_response(resp))


from argparse import ArgumentParser
import requests
import json

BASEURL = 'http://control.maxiv.lu.se/general/archiving/'
SEARCHURL = BASEURL + 'search'
QUERYURL = BASEURL + 'query'
CONTROLURL = "g-v-csdb-0.maxiv.lu.se:10000"

payload = {'targets': [{'target': "r1-d111210cab01/mag/psib-12/current", 'cs': "g-v-csdb-0.maxiv.lu.se:10000"}], 'range': {'from': "2018-06-16T15:00:00", 'to': "2018-06-16T15:30:00"}}
payload = {'cs': 'g-v-csdb-0.maxiv.lu.se:10000', 'target': 'r1.*cab01.*current'}

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

if __name__=="__main__":
    parser = ArgumentParser(description='Get data from HDB++ archiver')
    parser.add_argument('query', type=str, default='', help='Signal to acquire')
    parser.add_argument('start', type=str, default='', help='Start')
    parser.add_argument('end', type=str, default='', help='End')
    parser.add_argument(
            '-s',
            '--searchterm',
            type=str,
            help='Search term (regex)',
            )

    args = parser.parse_args()

    r = requests.post(
            url=QUERYURL,
            json=makequerypayload(args.query, args.start, args.end)
            )
    print(json.loads(r.text))

    if args.searchterm is not None:
        r = requests.post(
                url=SEARCHURL,
                json=makesearchpayload(args.searchterm),
                )
        reply = json.loads(r.text)
        for val in reply:
            print(val)


from cassandra.cluster import Cluster
from cassandra.policies import AddressTranslator
from datetime import datetime, timedelta, time
from time import time as tic
from itertools import chain
from argparse import ArgumentParser, FileType, ArgumentTypeError
from pytz import timezone
import logging

UTC = timezone('UTC')
CET = timezone('CET')


def parse_async_results(results):
    result_sets = [res.result() for res in results]
    return list(chain(*result_sets))


class LowlevelSignal:
    """
    Represents a signal found in the HDB++ archiver.
    Each instance of this class contains its own session, and so
    can execute queries easily during its entire lifetime.

    This class should only be used when really low-level access
    to Cassandra is needed.  It will only work on the Green network.
    """
    def __init__(self, signal):
        self.att = signal
        self._att_id = None
        self._datatype = None
        DC_1 = ['172.16.2.50', '172.16.2.69', '172.16.2.70', '172.16.2.71',]
        DC_2 = ['172.16.2.32', '172.16.2.66', '172.16.2.34', '172.16.2.67',
                '172.16.2.51', '172.16.2.68']

        # Simple Network mapper to resolve green and blue addresses
        class NetworkAdressTranslator(AddressTranslator):
            def __init__(self, addr_map=None):
                self.addr_map = addr_map

            def translate(self, addr):
                new_addr = self.addr_map.get(addr)
                return new_addr

        # Blue to Green topology
        addr_map = {
            # Old nodes, need ip translation
            "172.16.2.31" : "10.0.107.93",
            "172.16.2.32" : "10.0.107.94",
            "172.16.2.33" : "10.0.107.95",
            "172.16.2.34" : "10.0.107.96",
            "172.16.2.50" : "10.0.107.98",
            "172.16.2.51" : "10.0.107.99",
            # New nodes, IP forwarding is configured on the network
            "172.16.2.66" : "172.16.2.66",
            "172.16.2.67" : "172.16.2.67",
            "172.16.2.68" : "172.16.2.68",
            "172.16.2.69" : "172.16.2.69",
            "172.16.2.70" : "172.16.2.70",
            "172.16.2.71" : "172.16.2.71",
        }

        # Convert cluster points to local network hosts
        self.hdb_cluster = [addr_map[host] for host in DC_1 + DC_2]
        self.translator = NetworkAdressTranslator(addr_map)

        hosts = self.hdb_cluster
        translator = self.translator
        cluster = Cluster(
                hosts,
                connect_timeout=1,
                address_translator=translator
                )
        self.session = cluster.connect('hdb')
        self.id_future = self.session.execute_async(self.conf_query)
        self.datatype_future = self.session.execute_async(self.datatype_query)

    @property
    def att_id(self):
        """The attribute ID of this record in the archiver"""
        if not self._att_id:
            result = self.id_future.result()
            self._att_id = result[0].att_conf_id
        return self._att_id

    @property
    def datatype(self):
        """The type of the date in the archiver"""
        if not self._datatype:
            result = self.datatype_future.result()
            self._datatype = result[0].data_type
        return self._datatype

    @property
    def conf_query(self):
        """The query that returns the attribute ID"""
        query = "SELECT att_conf_id FROM att_conf "
        query += "WHERE att_name='{}' ALLOW FILTERING"
        return query.format(self.att)

    @property
    def datatype_query(self):
        """The query that returns the data-type of the attribute"""
        query = "SELECT data_type FROM att_conf "
        query += "WHERE att_conf_id={} ALLOW FILTERING"
        return query.format(self.att_id)

    def data_query(self, date, timerange):
        """
        Returns the query needed to return the data within the specified
        timerange on the specified date.
        Note that both the date and timerange are always required, and that
        the timerange must be a 2-tuple of datetimes (not just times)
        That the date of the timerange objects matches the date input
        is not checked...
        """
        if timerange[0] is None:
            start = datetime.combine(date, time(0,0,0))
        else:
            start = timerange[0]
        if timerange[1] is None:
            end = datetime.combine(date, time(23, 59, 59))
        else:
            end = timerange[1]
        query = "SELECT * from att_{} WHERE att_conf_id={} "
        query += "and period='{}' and data_time>'{}' and data_time<'{}'"
        query = query.format(self.datatype, self.att_id, date, start, end)
        return query

    def get_data(self, datetimerange):
        """Synchronously request the data within this datetime-range"""
        periods, timeranges = self.parse_datetimerange(datetimerange)
        results = [self.session.execute(self.data_query(date=p, timerange=t))
                for p, t in zip(periods, timeranges)]
        return list(chain(*results))

    def async_get_data(self, datetimerange):
        """Asynchronously request the data within this datetime-range"""
        periods, timeranges = self.parse_datetimerange(datetimerange)
        resultsets = [self.session.execute_async(
                            self.data_query(date=p, timerange=t)
                            )
                for p, t in zip(periods, timeranges)]
        resultsets = [resultset.result() for resultset in resultsets]
        data = []
        t = []
        for resultset in resultsets:
            for row in resultset:
                data.append(row.value_r)
                t.append(row.data_time)
        return t, data

    def parse_datetimerange(self, datetimerange):
        """
        A helper function to parse the datetimerange into the
        necessary list of dates and datetime tuples
        """
        start = datetimerange[0]
        end = datetimerange[1]
        if start.date() == end.date():
            # timerange doesn't cross midnight
            period = [start.date()]
            timeranges = [(start, end)]
            return period, timeranges
        periods = []
        runningdate = start.date()
        periods.append(runningdate)
        timeranges = [(start, None)]
        while not runningdate == end.date():
            runningdate += timedelta(days=1)
            periods.append(runningdate)
            timeranges.append((None, None))
        timeranges[-1] = (None, end)
        return periods, timeranges

def parse_response(signame, resp):
    timelist = resp[0]
    datalist = resp[1]
    output = []
    datetime_str = datetime.isoformat(datetime.now(), sep=':')
    output.append('"# DATASET= tango://' + signame + '"')
    output.append('"# SNAPSHOT_TIME= ' + datetime_str + '"')
    for t,dat in zip(timelist, datalist):
        timestamp = t.strftime("%Y-%m-%d_%H:%M:%S.%f")
        output.append('{} {}'.format(timestamp, dat))
    return '\n'.join(output) + '\n'


if __name__=="__main__":
    def interval_value(val):
        if not re.match('\d+\.*\d*[smh]$', str(val)):
            raise ArgumentTypeError(
                    'INTERVAL must be a number followed by s, m, h, or d'
                    )
        return val

    parser = ArgumentParser(
            description='Low-level API to the HDB++ archiver',
            )
    parser.add_argument(
            'signal', type=str,
            help='''
            Signal to acquire. Must be a single name, with no wildcards.
            It is not possible to aquire multiple signals with one call.
            ''',
           )
    parser.add_argument(
            '-f', '--file', type=str,
            help='''
            Root name of file in which to save the data.
            A single file will be created with the name FILE.dat.
            If the file already exists, it will be overwritten, so
            use with care. Use of this option suppresses standard output.
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
    verbose = True
    logger = logging.getLogger(__name__)
    if verbose:
        logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - - %(levelname)s - %(message)s'
                )
    else:
        logging.basicConfig(
                format='%(asctime)s - - %(levelname)s - %(message)s'
                )

    start_naive = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%S")
    end_naive = datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%S")
    start_cet = CET.localize(start_naive)
    end_cet = CET.localize(end_naive)
    start_utc = start_cet.astimezone(UTC)
    end_utc = end_cet.astimezone(UTC)

    att = args.signal
    a = LowlevelSignal(att)

    print(start_utc, end_utc)

    res = a.async_get_data((start_utc, end_utc))
    resp = parse_response(args.signal, res)

    if args.file:
        filename = args.file + '.dat'
        with open(filename, 'w') as f:
            f.write(resp)
    else:
        print(resp)



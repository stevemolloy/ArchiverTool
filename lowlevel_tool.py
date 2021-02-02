from cassandra.cluster import Cluster
from cassandra.policies import AddressTranslator
from datetime import datetime, timedelta, time
from time import time as tic
from itertools import chain


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
        return [self.session.execute_async(
                            self.data_query(date=p, timerange=t)
                            )
                for p, t in zip(periods, timeranges)]

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

if __name__=="__main__":
    att = 'r1-101s/dia/dcct-01/current'
    a = LowlevelSignal(att)

    start = datetime(2019, 1, 22, 17, 0, 0)
    end = datetime(2019, 1, 25, 15, 0, 0)
    periods, timeranges = a.parse_datetimerange((start, end))

    res = a.async_get_data((start, end))
    for row in res:
        print(row.result())

    # async_runtime, sync_runtime, speedup = [], [], []
    # for iteration in range(20):
    #     t = tic()
    #     results = []
    #     results.append(a.get_data((datetime(2019, 2, 13, 0, 5, 0),
    #                                       datetime(2019, 2, 13, 23, 30, 0)))[0])
    #     results.append(a.get_data((datetime(2019, 2, 14, 0, 5, 0),
    #                                       datetime(2019, 2, 14, 23, 30, 0)))[0])
    #     results.append(a.get_data((datetime(2019, 2, 15, 0, 5, 0),
    #                                       datetime(2019, 2, 15, 23, 30, 0)))[0])
    #     results.append(a.get_data((datetime(2019, 2, 16, 0, 5, 0),
    #                                       datetime(2019, 2, 16, 23, 30, 0)))[0])
    #     results.append(a.get_data((datetime(2019, 2, 17, 0, 5, 0),
    #                                       datetime(2019, 2, 17, 23, 30, 0)))[0])
    #     results.append(a.get_data((datetime(2019, 2, 18, 0, 5, 0),
    #                                       datetime(2019, 2, 18, 23, 30, 0)))[0])
    #     sync_runtime.append(tic() - t)

    #     t = tic()
    #     futures = []
    #     futures.append(a.async_get_data((datetime(2019, 2, 13, 0, 5, 0),
    #                                             datetime(2019, 2, 13, 23, 30, 0)))[0])
    #     futures.append(a.async_get_data((datetime(2019, 2, 14, 0, 5, 0),
    #                                             datetime(2019, 2, 14, 23, 30, 0)))[0])
    #     futures.append(a.async_get_data((datetime(2019, 2, 15, 0, 5, 0),
    #                                             datetime(2019, 2, 15, 23, 30, 0)))[0])
    #     futures.append(a.async_get_data((datetime(2019, 2, 16, 0, 5, 0),
    #                                             datetime(2019, 2, 16, 23, 30, 0)))[0])
    #     futures.append(a.async_get_data((datetime(2019, 2, 17, 0, 5, 0),
    #                                             datetime(2019, 2, 17, 23, 30, 0)))[0])
    #     futures.append(a.async_get_data((datetime(2019, 2, 18, 0, 5, 0),
    #                                             datetime(2019, 2, 18, 23, 30, 0)))[0])
    #     results = [fut.result() for fut in futures]
    #     async_runtime.append(tic() - t)

    #     print(iteration, ': Speedup: {}'.format(sync_runtime[-1] - async_runtime[-1]))
    #     speedup.append(sync_runtime[-1] - async_runtime[-1])

    # print('\n')
    # print('Async mean = {:0.3f}'.format(sum(async_runtime) / len(async_runtime)))
    # print('Sync mean = {:0.3f}'.format(sum(sync_runtime) / len(sync_runtime)))
    # print('Speedup mean = {:0.3f}'.format(sum(speedup) / len(speedup)))


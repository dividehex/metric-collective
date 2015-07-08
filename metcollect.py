# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import win32serviceutil
import win32service
import win32event
import servicemanager

import psutil
import socket
import time
import logging
import threading
import Queue
import ConfigParser
import os
import traceback
import sys
from string import maketrans

cfg = None
logger = None

def initialize_logger():
    try:
        local_log_file = cfg.get('LOG','file')
    except ConfigParser.Error:
        local_log_file = os.path.join(os.path.dirname(sys.argv[0]), "metcollect.log")
    logger = logging.getLogger('metcollect')

    try:
        cfg.get('LOG','debug')
    except ConfigParser.Error:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(local_log_file)
    fh.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s[%(levelname)s]: %(message)s')
    fh.setFormatter(file_formatter)
    logger.addHandler(fh)
    return logger


def init_config():
    cfg_file = os.path.join(os.path.dirname(sys.argv[0]), "metcollect.ini")
    cfg = ConfigParser.SafeConfigParser(allow_no_value=True)
    try:
        cfg.readfp(open(cfg_file))
    except Exception:
        sys.exit(-1)
    return cfg


class MetricWorkCls(object):

    name = None
    interval = 60
    last_poll_time = 0
    illegal_chars = u' !"#%\'()*+,./:;<=>?@[\]^`{|}~'

    def __init__(self, ):
        try:
            self.interval = cfg.getint('MAIN','interval')
        except ConfigParser.Error:
            pass
        self.trans_table = dict((ord(char), u'_') for char in self.illegal_chars)


    def timeToWait(self, ):
        """Returns time to wait for next metric poll"""
        time_passed = time.time() - self.last_poll_time

        # Time since last poll has exceeded interval_time; don't wait, poll now
        if time_passed > self.interval:
            return 0
        return self.interval - time_passed

    def sanitize_path(self, path):
        """Sanitize path names to be carbon ready"""
        return path.translate(self.trans_table)

    def getName(self, ):
        return self.name

    def get_metric(self, ):
        raise NotImplementedError

    def write_metric(self, ):
        raise NotImplementedError

    def teardown(self, ):
        pass


class MetricReadCls_cpu(MetricWorkCls):

    name = 'cpu'


    def get_metric(self, ):
        metric_out = []
        timestamp = time.time()
        raw_metric = psutil.cpu_times(percpu=True)

        n = 0
        for cpu in raw_metric:
            prefix = '%s.%s.' % (self.name, n)
            metric_out.append((prefix + 'user', cpu.user, timestamp))
            metric_out.append((prefix + 'system', cpu.system, timestamp))
            metric_out.append((prefix + 'idle', cpu.idle, timestamp))
            n+=1
        self.last_poll_time = timestamp
        return metric_out


class MetricReadCls_memory(MetricWorkCls):

    name = 'memory'

    def get_metric(self, ):
        metric_out = []
        prefix = '%s.' % (self.name)
        timestamp = time.time()
        mem = psutil.virtual_memory()

        metric_out.append((prefix + 'total', mem.total, timestamp))
        metric_out.append((prefix + 'available', mem.available, timestamp))
        metric_out.append((prefix + 'percent', mem.percent, timestamp))
        metric_out.append((prefix + 'used', mem.used, timestamp))
        metric_out.append((prefix + 'free', mem.free, timestamp))
        self.last_poll_time = timestamp
        return metric_out


class MetricReadCls_swap(MetricWorkCls):

    name = 'swap'

    def get_metric(self, ):
        metric_out = []
        prefix = '%s.' % (self.name)
        timestamp = time.time()
        swap = psutil.swap_memory()

        metric_out.append((prefix + 'total', swap.total, timestamp))
        metric_out.append((prefix + 'used', swap.used, timestamp))
        metric_out.append((prefix + 'free', swap.free, timestamp))
        metric_out.append((prefix + 'percent', swap.percent, timestamp))
        self.last_poll_time = timestamp
        return metric_out

class MetricReadCls_disk_io(MetricWorkCls):

    name = 'disk_io'

    def get_metric(self, ):
        metric_out = []
        timestamp = time.time()
        disk_io = psutil.disk_io_counters(perdisk=True)
        for disk in disk_io:
            prefix = '%s.%s.' % (self.name, disk)
            metric_out.append((prefix + 'read_count', disk_io[disk].read_count, timestamp))
            metric_out.append((prefix + 'write_count', disk_io[disk].write_count, timestamp))
            metric_out.append((prefix + 'read_bytes', disk_io[disk].read_bytes, timestamp))
            metric_out.append((prefix + 'write_bytes', disk_io[disk].write_bytes, timestamp))
            metric_out.append((prefix + 'read_time', disk_io[disk].read_time, timestamp))
            metric_out.append((prefix + 'write_time', disk_io[disk].write_time, timestamp))
        self.last_poll_time = timestamp
        return metric_out


class MetricReadCls_disk_usage(MetricWorkCls):

    name = 'disk_usage'

    def get_metric(self, ):
        metric_out = []
        parts_mp = []

        all_parts_lst = psutil.disk_partitions(all=True)
        for parts in all_parts_lst:
            if 'fixed' in parts.opts:
                parts_mp.append(parts.mountpoint)

        for mp in parts_mp:
            timestamp = time.time()
            disk_usage = psutil.disk_usage(mp)
            prefix = '%s.%s.' % (self.name, mp.strip(':\\'))
            metric_out.append((prefix + 'total', disk_usage.total, timestamp))
            metric_out.append((prefix + 'used', disk_usage.used, timestamp))
            metric_out.append((prefix + 'free', disk_usage.free, timestamp))
            metric_out.append((prefix + 'percent', disk_usage.percent, timestamp))
        self.last_poll_time = timestamp
        return metric_out

class MetricReadCls_net_io_counters(MetricWorkCls):


    name = 'net_io_counters'

    def get_metric(self, ):
        metric_out = []
        timestamp = time.time()
        net_io = psutil.net_io_counters(pernic=True)
        for inet in net_io:
            prefix = '%s.%s.' % (self.name, self.sanitize_path(inet))
            metric_out.append((prefix + 'bytes_sent', net_io[inet].bytes_sent, timestamp))
            metric_out.append((prefix + 'bytes_recv', net_io[inet].bytes_recv, timestamp))
            metric_out.append((prefix + 'packets_sent', net_io[inet].packets_sent, timestamp))
            metric_out.append((prefix + 'packets_recv', net_io[inet].packets_recv, timestamp))
            metric_out.append((prefix + 'errin', net_io[inet].errin, timestamp))
            metric_out.append((prefix + 'errout', net_io[inet].errout, timestamp))
            metric_out.append((prefix + 'dropin', net_io[inet].dropin, timestamp))
            metric_out.append((prefix + 'dropout', net_io[inet].dropout, timestamp))
        self.last_poll_time = timestamp
        return metric_out

class MetricReadCls_uptime(MetricWorkCls):


    name = 'uptime'

    def get_metric(self, ):
        timestamp = time.time()
        uptime = time.time() - psutil.boot_time()
        prefix = '%s.' % (self.name)
        self.last_poll_time = timestamp
        return [(prefix + 'uptime', uptime, timestamp)]

class MetricWriteCls_graphite(MetricWorkCls):

    name = 'graphite'
    backoff_delay = 30
    backoff = 0
    sock = None

    def __init__(self, ):

        try:
            self.graphite_server = cfg.get('GRAPHITE','server')
            self.graphite_port = cfg.getint('GRAPHITE','port')
            self.prefix = cfg.get('GRAPHITE','prefix')
        except ConfigParser.Error:
            logger.error('missing requied settings in [GRAPHITE]')
            logger.error('EXITING')
            sys.exit(-1)

        try:
            self.hostname = socket.gethostname()
            self.fqdn = socket.getfqdn(self.hostname).lower()
        except socket.error:
            logger.error('Could not resolve local fqdn')
            logger.error('EXITING')
            sys.exit(-1)

        self.instance_path = self.prefix + self.fqdn.replace('.', '_') + '.'
        super(MetricWriteCls_graphite, self).__init__()

    def connect(self, ):
        """
        Initialize sockect and establish connection.  If socket
        already exists, then close existing socket and re-establish
        a new socket and connection
        """
        if self.sock:
            self.sock.close()
            self.sock = None

        logger.info('Opening sock to %s:%s' % (self.graphite_server, self.graphite_port))
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.graphite_server, self.graphite_port))
        except:
            logger.error('Unable to connect to %s:%s' % (self.graphite_server, self.graphite_port))
            logger.error(traceback.format_exc())


    def write_metric(self, metric_tpl):
        metric_line = self.format_plaintxt(metric_tpl)

        # We drop metrics if they fail to send.  This prevents
        # the graphite/carbon server from being pounded
        self.send_metric(metric_line)

    def format_plaintxt(self, metric_tpl):
        metric_path, metric, timestamp = metric_tpl
        metric_path = self.instance_path + metric_path
        return '%s %f %d\n' % (metric_path, metric, timestamp)

    def send_metric(self, metric_line):
        """
        This method takes a single line plaintxt formatted metric
        and attempts to send it via tcp.  If the socket isn't initialized
        it will call the connect method to establish a connection.  Any
        socket errors will cause the socket to be closed and re-established
        before a retry.  3 attempts will be made before giving up, setting a
        backoff timer for 10 secs and returning the backoff timer in secs.
        Subsquent calls will return time left (in secs) on the back off timer
        to allow the calling method to decide whether to wait and requeue the
        failed metric for sending or drop the metric.  If a send is successful,
        None is returned.
        """

        logger.debug('Sending %s' % (metric_line))
        retries = 0

        # Return seconds left on backoff timer
        if self.backoff > time.time():
            return self.backoff - time.time()

        # Make sure connection is established
        if not self.sock:
            self.connect()

        while retries < 2:
            try:
                self.sock.sendall(metric_line)
            except socket.timeout:
                retries += 1
            except socket.error:
                retries += 1
                # Reinitialize socket and establish new connection
                self.connect()
            else:
                # Send was successful; Return None
                return None

        # Send failed after retries, set and return backoff timer
        self.backoff = time.time() + self.backoff_delay
        return self.backoff_delay

    def teardown(self, ):
        logger.info('Closing socket to graphite server')
        if self.sock:
            self.sock.close()


class MetricWorkThread(threading.Thread):



    def __init__(self, metrics_queue, start_delay, work_obj):
        super(MetricWorkThread, self).__init__()
        self.stop_event = threading.Event()
        self.metrics_queue = metrics_queue
        self.start_delay = start_delay
        self.work_obj = work_obj

        # set thread name based on metric name
        self.name = '{0}-{1}'.format(self.getName(), self.work_obj.getName())
        logger.info('Initalizing %s' % (self.name))

    def start(self, ):
        logger.info('Start %s' % (self.name))
        super(MetricWorkThread, self).start()

    def run(self, ):
        time.sleep(self.start_delay)

        # Execute main workload method and then teardown
        # We also catch any raised errors and log the traceback
        try:
            self._workload()
            self._teardown()
        except:
            logger.error(traceback.format_exc())
        logger.info('%s died gracefully' % (self.name))

    def _workload(self, ):
        raise NotImplementedError

    def _teardown(self, ):
        raise NotImplementedError


class MetricReader(MetricWorkThread):
    """ Reader Thread """

    def _workload(self, ):
        """
        Loop reading metrics from work objects and wait on time returned
        by work objects timeToWait() method
        """

        while not self.stop_event.wait(self.work_obj.timeToWait()):
            for metric_tuple in self.work_obj.get_metric():
                logger.debug(
                    '%s[%s]: %s' % (self.name, self.work_obj.interval, str(metric_tuple)))
                self.metrics_queue.put(metric_tuple)

    def join(self, timeout=None):
        logger.info('Requesting %s die' % (self.name))
        self.stop_event.set()
        super(MetricWorkThread, self).join(timeout)

    def _teardown(self, ):
        self.work_obj.teardown()


class MetricWriter(MetricWorkThread):
    """ Writer Thread """

    def _workload(self, ):
        """
        Loop popping metrics from main queue and sending them
        to the write_metric method of the work object.  Reading from
        the queue blocks until an object is retieved.  When None object
        is popped, read loop ends when the queue is finally empty.
        """
        stop_flag = False
        # Short circuit until stop_flag, then end when queue is empty
        while not stop_flag or not self.metrics_queue.empty():
            metric_tpl = self.metrics_queue.get(True)
            if metric_tpl == None:
                stop_flag = True
            else:
                self.work_obj.write_metric(metric_tpl)

    def join(self, timeout=None):
        logger.info('Requesting %s die' % (self.name))
        self.metrics_queue.put(None)
        super(MetricWorkThread, self).join(timeout)

    def _teardown(self, ):
        self.work_obj.teardown()


class MetricCollectiveDriver:

    read_thread_pool = []
    write_thread_pool = []

    read_work_objs = []
    write_work_objs = []

    def __init__(self):
        self.metric_queue = Queue.Queue()
        read_cls = []

        # list of available read modules
        avail_read_mods = ['cpu', 'memory', 'swap', 'disk_io','disk_usage',
                          'net_io_counters', 'uptime',]


        try:
            modules_en = cfg.options('READ_MODULES_ENABLED')
        except:
            logger.error('READ_MODULES_ENABLED section not found in config file')
            logger.error('EXITING')
            sys.exit(-1)

        # assemble a list of enabled modules
        for r_module in modules_en:
            if r_module in avail_read_mods:
                read_cls.append('MetricReadCls_' + r_module)
            else:
                logger.warning('Unknown module in [READ MODULES ENABLED]')

        if len(read_cls) == 0:
            logger.error('0 valid read modules in [READ MODULES ENABLED]')
            logger.error('EXITING')
            sys.exit(-1)

        write_cls = ['MetricWriteCls_graphite']
        self.init_work_objs(read_cls, write_cls)
        self.init_metrics_threads()


    def start(self):
        logger.info(
            'Driver starting %s read thread(s)' % (len(self.read_thread_pool)))
        # start read threads
        for r_thread in self.read_thread_pool:
            r_thread.start()
        # start write threads
        logger.info(
            'Driver starting %s write thread(s)' % (len(self.write_thread_pool)))
        for w_thread in self.write_thread_pool:
            w_thread.start()

    def stop(self):
        # stop read threads
        logger.info(
            'Driver stopping %s read thread(s)' % (len(self.read_thread_pool)))
        for r_thread in self.read_thread_pool:
            r_thread.join()
        # stop write threads
        logger.info(
            'Driver stopping %s write thread(s)' % (len(self.write_thread_pool)))
        for w_thread in self.write_thread_pool:
            w_thread.join()


    def init_work_objs(self, read_cls, write_cls):
        module = __import__('metcollect')

        for r_cls in read_cls:
            self.read_work_objs.append(getattr(module, r_cls)())

        for w_cls in write_cls:
            self.write_work_objs.append(getattr(module, w_cls)())


    def init_metrics_threads(self, ):
        start_delay = 1
        logger.info(
            'Spawning %s read thread(s)' % (len(self.read_work_objs)))
        for work_obj in self.read_work_objs:
            t_obj = MetricReader(self.metric_queue, start_delay, work_obj)
            self.read_thread_pool.append(t_obj)

        logger.info(
            'Spawning %s write thread(s)' % (len(self.write_work_objs)))
        for work_obj in self.write_work_objs:
            t_obj = MetricWriter(self.metric_queue, start_delay, work_obj)
            self.write_thread_pool.append(t_obj)



class AppServerSvc (win32serviceutil.ServiceFramework):
    _svc_name_ = "metcollect"
    _svc_display_name_ = "Metric Collective"
    _svc_description_ = "Collect system metrics and send to Graphite"

    def __init__(self,args):
        win32serviceutil.ServiceFramework.__init__(self,args)
        self.hWaitStop = win32event.CreateEvent(None,0,0,None)

    def SvcStop(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STOPPING,
                              (self._svc_name_,''))
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_,''))
        self.main()

    def main(self):
        global cfg, logger
        cfg = init_config()
        logger = initialize_logger()
        logger.info('Initializing Metrics Collection Driver')
        self.drv = MetricCollectiveDriver()
        self.drv.start()

        # check back every 10 mins
        self.timeout = 10*60*1000
        while True:
            rc = win32event.WaitForSingleObject(self.hWaitStop, self.timeout)
            if rc == win32event.WAIT_OBJECT_0:
                break
        self.drv.stop()
        logger.info('Ending Metrics Collection Driver')
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STOPPED,
                              (self._svc_name_,''))

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(AppServerSvc)


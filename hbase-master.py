# coding=utf-8

"""
Diamond collector for HBase Master metrics
"""

from urllib2 import urlopen
import diamond.collector
import json


def bean_metric(prefix):
    def big_wrapper(func):
        def wrapper(*args, **kwargs):
            itr = func(*args, **kwargs)
            while True:
                try:
                    path, value = itr.next()
                    if path.lower() == "modelertype" or \
                            path.lower().find("tag.") == 0 or \
                            path.lower() == "name" or \
                            path.lower() == "objectname":
                        continue
                    if not type(value) is int:
                        continue
                    path = path.replace(".", "_")
                    path = ".".join((prefix, path))
                    yield (path, value)
                except StopIteration:
                    break
        return wrapper
    return big_wrapper


class HBaseMasterCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        self.BEANS_MAP = {
            "Hadoop:service=HBase,name=Master,sub=Balancer": self.hbase_master_balancer,
            "Hadoop:service=HBase,name=Master,sub=AssignmentManger": self.hbase_master_assignmentmanger,
            "java.lang:type=Runtime": self.java_runtime,
            "java.lang:type=Threading": self.java_threading,
            "java.lang:type=OperatingSystem": self.java_operatingsystem,
            "Hadoop:service=HBase,name=MetricsSystem,sub=Stats": self.hbase_metricsystem_stats,
            "java.lang:type=MemoryPool,name=Code Cache": self.java_memorypool_codecache,
            "java.nio:type=BufferPool,name=direct": self.java_bufferpool_direct,
            "java.lang:type=GarbageCollector,name=G1 Young Generation": self.java_gc_G1_young,
            "java.lang:type=MemoryPool,name=G1 Old Gen": self.java_memorypool_G1_old,
            "java.lang:type=GarbageCollector,name=G1 Old Generation": self.java_gc_G1_old,
            "Hadoop:service=HBase,name=Master,sub=FileSystem": self.hbase_master_filesystem,
            "java.lang:type=MemoryPool,name=G1 Survivor Space": self.java_memorypool_G1_survivorspace,
            "java.lang:type=MemoryPool,name=Metaspace": self.java_memorypool_metaspace,
            "Hadoop:service=HBase,name=Master,sub=Server": self.hbase_master_server,
            "Hadoop:service=HBase,name=JvmMetrics": self.hbase_jvmmetrics,
            "java.lang:type=Memory": self.java_memory,
            "Hadoop:service=HBase,name=Master,sub=IPC": self.hbase_master_ipc,
            "Hadoop:service=HBase,name=UgiMetrics": self.hbase_ugimetrics,
            "Hadoop:service=HBase,name=Master,sub=Snapshots": self.hbase_master_snapshots,
            "Hadoop:service=HBase,name=Master,sub=Procedure": self.hbase_master_procedure
        }
        super(HBaseMasterCollector, self).__init__(*args, **kwargs)

    def get_default_config_help(self):
        config_help = super(HBaseMasterCollector, self).get_default_config_help()
        config_help.update({
            'url': 'URL of jxm metrics',
            'metrics': 'List of beans name'
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(HBaseMasterCollector, self).get_default_config()
        config.update({
            'path':     'hbase.master',
            'url': 'http://127.0.0.1:60010/jmx',
            'metrics':  self.BEANS_MAP.keys()
        })
        return config

    def collect(self):
        url = self.config['url']
        try:
            response = urlopen(url)
            content = json.loads(response.read().decode())

            for bean in content['beans']:
                bean_name = bean['name']
                if not bean_name in self.config['metrics']:
                    continue
                func = self.BEANS_MAP[bean_name]
                for path, value in func(bean):
                    self.publish(path, value)
        except URLError:
            pass

    @bean_metric("balancer")
    def hbase_master_balancer(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("assignmentmanger")
    def hbase_master_assignmentmanger(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("runtime")
    def java_runtime(self, data):
        allowed = ("StartTime", "Uptime")
        for key, value in data.iteritems():
            if key in allowed:
                yield (key, value)

    @bean_metric("threading")
    def java_threading(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("operatingsystem")
    def java_operatingsystem(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("metricsystem_stats")
    def hbase_metricsystem_stats(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("memorypool_codecache")
    def java_memorypool_codecache(self, data):
        for key, value in data.iteritems():
            yield (key, value)
        for key in ("Usage", "PeakUsage"):
            for k, v in data[key].iteritems():
                yield (".".join((key, k)), v)

    @bean_metric("bufferpool_direct")
    def java_bufferpool_direct(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("gc_G1_young")
    def java_gc_G1_young(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("memorypool_G1_old")
    def java_memorypool_G1_old(self, data):
        for key, value in data.iteritems():
            yield (key, value)
        for key in ("Usage", "PeakUsage", "CollectionUsage"):
            for k, v in data[key].iteritems():
                yield (".".join((key, k)), v)

    @bean_metric("gc_G1_old")
    def java_gc_G1_old(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("filesystem")
    def hbase_master_filesystem(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("memorypool_G1_survivorspace")
    def java_memorypool_G1_survivorspace(self, data):
        for key, value in data.iteritems():
            yield (key, value)
        for key in ("Usage", "PeakUsage", "CollectionUsage"):
            for k, v in data[key].iteritems():
                yield (".".join((key, k)), v)

    @bean_metric("memorypool_metaspace")
    def java_memorypool_metaspace(self, data):
        for key, value in data.iteritems():
            yield (key, value)
        for key in ("Usage", "PeakUsage"):
            for k, v in data[key].iteritems():
                yield (".".join((key, k)), v)

    @bean_metric("server")
    def hbase_master_server(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("jvmmetrics")
    def hbase_jvmmetrics(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("memory")
    def java_memory(self, data):
        for key, value in data.iteritems():
            yield (key, value)
        for key in ("HeapMemoryUsage", "NonHeapMemoryUsage"):
            for k, v in data[key].iteritems():
                yield (".".join((key, k)), v)

    @bean_metric("ipc")
    def hbase_master_ipc(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("ugimetrics")
    def hbase_ugimetrics(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("snapshots")
    def hbase_master_snapshots(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("procedure")
    def hbase_master_procedure(self, data):
        for key, value in data.iteritems():
            yield (key, value)

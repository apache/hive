#!/usr/bin/python

import sys,os,stat
import argparse
from json import loads as json_parse
from json import dumps as json_print
from os.path import exists, join, relpath
from time import gmtime, strftime
import shutil
import tarfile
import zipfile

from templates import yarnfile, runner

class LlapResource(object):
	def __init__(self, config):
		self.memory = config["hive.llap.daemon.memory.per.instance.mb"]
		self.cores = config["hive.llap.daemon.vcpus.per.instance"]
		size = config["hive.llap.daemon.yarn.container.mb"]
		# convert to Mb
		self.cache = config["hive.llap.io.memory.size"] / (1024*1024.0)
		self.direct = config["hive.llap.io.allocator.direct"]
		self.executors = config["hive.llap.daemon.num.executors"]
		self.min_cores = -1
		# compute heap + cache as final Xmx
		h = self.memory 
		if (not self.direct):
			h += self.cache
		if size == -1:
			print("Cannot determine the container size")
			sys.exit(1)
			return
		else:
			# do not mess with user input
			c = size
		self.container_size = c
		self.container_cores = self.cores
		self.heap_size = h

		if (not config.get("hive.llap.daemon.queue.name","")):
			self.queueString = ""
		else:
			self.queueString = config["hive.llap.daemon.queue.name"]

		if (not config.get("private.hive.llap.servicedriver.cluster.name")):
			self.clusterName="llap0"
		else:
			self.clusterName = config["private.hive.llap.servicedriver.cluster.name"]

	def __repr__(self):
		return "<LlapResource heap=%d container=%d>" % (self.heap_size, self.container_size)

def zipdir(path, zip, prefix="."):
	for root, dirs, files in os.walk(path):
		for file in files:
			src = join(root, file)
			dst = src.replace(path, prefix)
			zip.write(src, dst)

def service_appconfig_global_property(arg):
	kv = arg.split("=")
	if len(kv) != 2:
		raise argparse.ArgumentTypeError("Value must be split into two parts separated by =")
	return tuple(kv)

def construct_service_site_global_string(kvs):
	if not kvs:
		return ""
	kvs = [a[0] for a in kvs]
	return ",\n" + ",\n".join(["    %s:%s" % (json_print(k), json_print(v)) for (k,v) in kvs])

	
def main(args):
	version = os.getenv("HIVE_VERSION")
	if not version:
		version = strftime("%d%b%Y", gmtime())
	home = os.getenv("HIVE_HOME")
	output = "llap-yarn-%(version)s" % ({"version": version})
	parser = argparse.ArgumentParser()
	parser.add_argument("--instances", type=int, default=1)
	parser.add_argument("--output", default=output)
	parser.add_argument("--input", required=True)
	parser.add_argument("--args", default="")
	parser.add_argument("--name", default="llap0")
	parser.add_argument("--loglevel", default="INFO")
	parser.add_argument("--logger", default="query-routing")
	parser.add_argument("--service-am-container-mb", type=int, default=1024)
	parser.add_argument("--service-appconfig-global", nargs='*', type=service_appconfig_global_property, action='append')
	parser.add_argument("--service-keytab-dir", default="")
	parser.add_argument("--service-keytab", default="")
	parser.add_argument("--service-principal", default="")
	parser.add_argument("--service-default-keytab", dest='service_default_keytab', action='store_true')
	parser.add_argument("--service-placement", type=int, default=4)
	parser.add_argument("--health-percent", type=int, default=80)
	parser.add_argument("--health-time-window-secs", type=int, default=300)
	parser.add_argument("--health-init-delay-secs", type=int, default=400)
	parser.set_defaults(service_default_keytab=False)
	parser.add_argument("--startImmediately", dest='start_immediately', action='store_true')
	parser.add_argument("--javaChild", dest='java_child', action='store_true')
	parser.set_defaults(start_immediately=False)
	parser.set_defaults(java_child=False)
	# Unneeded here for now: parser.add_argument("--hiveconf", action='append')
	#parser.add_argument("--size") parser.add_argument("--xmx") parser.add_argument("--cache") parser.add_argument("--executors")
	(args, unknown_args) = parser.parse_known_args(args)
	if args.start_immediately and not args.java_child:
		sys.exit(0)
		return
	if args.java_child:
		print("%s Running as a child of LlapServiceDriver" % (strftime("%H:%M:%S", gmtime())))
	else:
		print("%s Running after LlapServiceDriver" % (strftime("%H:%M:%S", gmtime())))

	input = args.input
	output = args.output
	service_am_jvm_heapsize = max(args.service_am_container_mb * 0.8, args.service_am_container_mb - 1024)
	service_keytab_dir = args.service_keytab_dir
	service_keytab = args.service_keytab
	service_principal = args.service_principal

	config = json_parse(open(join(input, "config.json")).read())
	# set the defaults only if the defaults are enabled
	if args.service_default_keytab:
		if not service_keytab_dir:
			service_keytab_dir = config["hive.llap.hdfs.package.dir"] + "/keytabs/llap"
		if not service_keytab:
			service_keytab = "llap.keytab"
		if not service_principal:
			service_principal = "llap@EXAMPLE.COM"
	service_keytab_path = service_keytab_dir
	if service_keytab_path:
		if service_keytab:
			service_keytab_path += "/" + service_keytab
	else:
		service_keytab_path = service_keytab

	if not input:
		print("Cannot find input files")
		sys.exit(1)
		return
	java_home = config["java.home"]
	max_direct_memory = config["max_direct_memory"]

	resource = LlapResource(config)

	daemon_args = args.args

	# https://docs.python.org/3.0/whatsnew/3.0.html#integers
	if int(max_direct_memory) > 0:
		daemon_args = " -XX:MaxDirectMemorySize=%s %s" % (max_direct_memory, daemon_args)
	daemon_args = " -Dhttp.maxConnections=%s %s" % ((max(args.instances, resource.executors) + 1), daemon_args)
	vars = {
		"home" : home,
		"version" : version,
		"instances" : args.instances,
		"heap" : resource.heap_size,
		"container.mb" : resource.container_size,
		"container.cores" : resource.container_cores,
		"hadoop_home" : os.getenv("HADOOP_HOME"),
		"java_home" : java_home,
		"name" : resource.clusterName,
		"daemon_args" : daemon_args,
		"daemon_loglevel" : args.loglevel,
		"daemon_logger" : args.logger,
		"queue.string" : resource.queueString,
		"service.am.container.mb" : args.service_am_container_mb,
		"service_appconfig_global_append": construct_service_site_global_string(args.service_appconfig_global),
		"service_am_jvm_heapsize" : service_am_jvm_heapsize,
		"service_keytab_path" : service_keytab_path,
		"service_principal" : service_principal,
		"placement" : args.service_placement,
		"health_percent": args.health_percent,
		"health_time_window": args.health_time_window_secs,
		"health_init_delay": args.health_init_delay_secs,
		"hdfs_package_dir": config["hive.llap.hdfs.package.dir"]
	}

	if not exists(output):
		os.makedirs(output)

	src = join(home, "scripts", "llap", "bin")
	dst = join(input, "bin")
	if exists(dst):
		shutil.rmtree(dst)
	shutil.copytree(src, dst)

	# Make the llap tarball
	print("%s Prepared the files" % (strftime("%H:%M:%S", gmtime())))

	tarball = tarfile.open(join(output, "%s-%s.tar.gz" % (resource.clusterName, version)), "w:gz")
	# recursive add + -C chdir inside
	tarball.add(input, "")
	tarball.close()

	print("%s Packaged the files" % (strftime("%H:%M:%S", gmtime())))

	with open(join(output, "Yarnfile"), "w") as f:
		f.write(yarnfile % vars)

	with open(join(output, "run.sh"), "w") as f:
		f.write(runner % vars)
	os.chmod(join(output, "run.sh"), 0o700)
    # https://docs.python.org/3.0/whatsnew/3.0.html#integers

	if not args.java_child:
		print("%s Prepared %s/run.sh for running LLAP on YARN" % (strftime("%H:%M:%S", gmtime()), output))

if __name__ == "__main__":
	main(sys.argv[1:])
# vim: ai ts=4 noet sw=4 ft=python

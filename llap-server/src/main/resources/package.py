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

from templates import metainfo, appConfig, resources, runner

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
			print "Cannot determine the container size"
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
			self.queueString = "--queue "
			self.queueString += config["hive.llap.daemon.queue.name"]

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

def slider_appconfig_global_property(arg):
	kv = arg.split("=")
	if len(kv) != 2:
		raise argparse.ArgumentTypeError("Value must be split into two parts separated by =")
	return tuple(kv)

def construct_slider_site_global_string(kvs):
	if not kvs:
		return ""
	kvs = map(lambda a : a[0], kvs)
	return ",\n" + ",\n".join(["    %s:%s" % (json_print(k), json_print(v)) for (k,v) in kvs])

	
def main(args):
	version = os.getenv("HIVE_VERSION")
	if not version:
		version = strftime("%d%b%Y", gmtime()) 
	home = os.getenv("HIVE_HOME")
	output = "llap-slider-%(version)s" % ({"version": version})
	parser = argparse.ArgumentParser()
	parser.add_argument("--instances", type=int, default=1)
	parser.add_argument("--output", default=output)
	parser.add_argument("--input", required=True)
	parser.add_argument("--args", default="")
	parser.add_argument("--name", default="llap0")
	parser.add_argument("--loglevel", default="INFO")
	parser.add_argument("--logger", default="query-routing")
	parser.add_argument("--chaosmonkey", type=int, default=0)
	parser.add_argument("--slider-am-container-mb", type=int, default=1024)
	parser.add_argument("--slider-appconfig-global", nargs='*', type=slider_appconfig_global_property, action='append')
	parser.add_argument("--slider-keytab-dir", default="")
	parser.add_argument("--slider-keytab", default="")
	parser.add_argument("--slider-principal", default="")
	parser.add_argument("--slider-default-keytab", dest='slider_default_keytab', action='store_true')
	parser.add_argument("--slider-placement", type=int, default=4)
	parser.add_argument("--health-percent", type=int, default=80)
	parser.add_argument("--health-time-window-secs", type=int, default=300)
	parser.add_argument("--health-init-delay-secs", type=int, default=400)
	parser.set_defaults(slider_default_keytab=False)
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
		print "%s Running as a child of LlapServiceDriver" % (strftime("%H:%M:%S", gmtime()))
	else:
		print "%s Running after LlapServiceDriver" % (strftime("%H:%M:%S", gmtime()))

	input = args.input
	output = args.output
	slider_am_jvm_heapsize = max(args.slider_am_container_mb * 0.8, args.slider_am_container_mb - 1024)
	slider_keytab_dir = args.slider_keytab_dir
	slider_keytab = args.slider_keytab
	slider_principal = args.slider_principal
	# set the defaults only if the defaults are enabled
	if args.slider_default_keytab:
		if not slider_keytab_dir:
			slider_keytab_dir = ".slider/keytabs/llap"
		if not slider_keytab:
			slider_keytab = "llap.keytab"
		if not slider_principal:
			slider_principal = "llap@EXAMPLE.COM"
	if not input:
		print "Cannot find input files"
		sys.exit(1)
		return
	config = json_parse(open(join(input, "config.json")).read())
	java_home = config["java.home"]
	max_direct_memory = config["max_direct_memory"]

	resource = LlapResource(config)

	daemon_args = args.args
	if long(max_direct_memory) > 0:
		daemon_args = " -XX:MaxDirectMemorySize=%s %s" % (max_direct_memory, daemon_args)
	daemon_args = " -Dhttp.maxConnections=%s %s" % ((max(args.instances, resource.executors) + 1), daemon_args)
	# 5% container failure every monkey_interval seconds
	monkey_percentage = 5 # 5%
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
		"monkey_interval" : args.chaosmonkey,
		"monkey_percentage" : monkey_percentage,
		"monkey_enabled" : args.chaosmonkey > 0,
		"slider.am.container.mb" : args.slider_am_container_mb,
		"slider_appconfig_global_append": construct_slider_site_global_string(args.slider_appconfig_global),
		"slider_am_jvm_heapsize" : slider_am_jvm_heapsize,
		"slider_keytab_dir" : slider_keytab_dir,
		"slider_keytab" : slider_keytab,
		"slider_principal" : slider_principal,
		"placement" : args.slider_placement,
                "health_percent": args.health_percent,
                "health_time_window": args.health_time_window_secs,
                "health_init_delay": args.health_init_delay_secs
	}
	
	if not exists(output):
		os.makedirs(output)
	
	src = join(home, "scripts", "llap", "bin")
	dst = join(input, "bin")
	if exists(dst):
		shutil.rmtree(dst)
	shutil.copytree(src, dst)

	# Make the zip package
	tmp = join(output, "tmp")
	pkg = join(tmp, "package")

	src = join(home, "scripts", "llap", "slider")
	dst = join(pkg, "scripts")
	if exists(dst):
		shutil.rmtree(dst)
	shutil.copytree(src, dst)

	with open(join(tmp, "metainfo.xml"),"w") as f:
		f.write(metainfo % vars)

	os.mkdir(join(pkg, "files"))
	print "%s Prepared the files" % (strftime("%H:%M:%S", gmtime()))

	tarball = tarfile.open(join(pkg, "files", "llap-%s.tar.gz" %  version), "w:gz")
	# recursive add + -C chdir inside
	tarball.add(input, "")
	tarball.close()

	zipped = zipfile.ZipFile(join(output, "llap-%s.zip" % version), "w")
	zipdir(tmp, zipped)
	zipped.close()
	print "%s Packaged the files" % (strftime("%H:%M:%S", gmtime()))

	# cleanup after making zip pkg
	shutil.rmtree(tmp)

	with open(join(output, "appConfig.json"), "w") as f:
		f.write(appConfig % vars)
	
	with open(join(output, "resources.json"), "w") as f:
		f.write(resources % vars)

	with open(join(output, "run.sh"), "w") as f:
		f.write(runner % vars)
	os.chmod(join(output, "run.sh"), 0700)

	if not args.java_child:
		print "%s Prepared %s/run.sh for running LLAP on Slider" % (strftime("%H:%M:%S", gmtime()), output)

if __name__ == "__main__":
	main(sys.argv[1:])
# vim: ai ts=4 noet sw=4 ft=python

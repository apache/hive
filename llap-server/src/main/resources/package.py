#!/usr/bin/python

import sys,os,stat
import argparse
from json import loads as json_parse
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
		self.min_mb = -1
		self.min_cores = -1
		# compute heap + cache as final Xmx
		h = self.memory 
		if (not self.direct):
			h += self.cache
		if size == -1:
			c = min(h*1.2, h + 1024) # + 1024 or 20%
			c += (self.direct and self.cache) or 0
			if self.min_mb > 0:
				c = c + c%self.min_mb
		else:
			# do not mess with user input
			c = size
		self.container_size = c
		self.container_cores = self.cores
		self.heap_size = h

	def __repr__(self):
		return "<LlapResource heap=%d container=%d>" % (self.heap_size, self.container_size)

def zipdir(path, zip, prefix="."):
	for root, dirs, files in os.walk(path):
		for file in files:
			src = join(root, file)
			dst = src.replace(path, prefix)
			zip.write(src, dst)
	
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
	parser.add_argument("--chaosmonkey", type=int, default=0)
	parser.add_argument("--slider-keytab-dir", default="")
	parser.add_argument("--slider-keytab", default="")
	parser.add_argument("--slider-principal", default="")
	parser.add_argument("--slider-default-keytab", dest='slider_default_keytab', action='store_true')
	parser.set_defaults(slider_default_keytab=False)
	# Unneeded here for now: parser.add_argument("--hiveconf", action='append')
	#parser.add_argument("--size") parser.add_argument("--xmx") parser.add_argument("--cache") parser.add_argument("--executors")
	(args, unknown_args) = parser.parse_known_args(args)
	input = args.input
	output = args.output
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
	resource = LlapResource(config)
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
		"name" : args.name,
		"daemon_args" : args.args,
		"daemon_loglevel" : args.loglevel,
		"monkey_interval" : args.chaosmonkey,
		"monkey_percentage" : monkey_percentage,
		"monkey_enabled" : args.chaosmonkey > 0,
		"slider_keytab_dir" : slider_keytab_dir,
		"slider_keytab" : slider_keytab,
		"slider_principal" : slider_principal
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
	tarball = tarfile.open(join(pkg, "files", "llap-%s.tar.gz" %  version), "w:gz")
	# recursive add + -C chdir inside
	tarball.add(input, "")
	tarball.close()

	zipped = zipfile.ZipFile(join(output, "llap-%s.zip" % version), "w")
	zipdir(tmp, zipped)
	zipped.close()

	# cleanup after making zip pkg
	shutil.rmtree(tmp)

	with open(join(output, "appConfig.json"), "w") as f:
		f.write(appConfig % vars)
	
	with open(join(output, "resources.json"), "w") as f:
		f.write(resources % vars)

	with open(join(output, "run.sh"), "w") as f:
		f.write(runner % vars)
	os.chmod(join(output, "run.sh"), 0700)

	print "Prepared %s/run.sh for running LLAP on Slider" % (output)

if __name__ == "__main__":
	main(sys.argv[1:])
# vim: ai ts=4 noet sw=4 ft=python

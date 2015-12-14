#!/usr/bin/python

import sys,os,stat
from getopt import getopt
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
		self.cache = config["hive.llap.io.cache.orc.size"] / (1024*1024.0)
		self.direct = config["hive.llap.io.cache.direct"]
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
	opts, args = getopt(args,"",["instances=","output=", "input=","args=","name=","loglevel=","chaosmonkey=","size=","xmx=", "cache=", "executors=","hiveconf="])
	version = os.getenv("HIVE_VERSION")
	if not version:
		version = strftime("%d%b%Y", gmtime()) 
	home = os.getenv("HIVE_HOME")
	output = "llap-slider-%(version)s" % ({"version": version})
	instances=1
	name = "llap0"
	d_args = ""
	d_loglevel = "INFO"
	input = None
	monkey = "0"
	for k,v in opts:
		if k in ("--input"):
			input = v
		elif k in ("--output"):
			output = v
		elif k in ("--instances"):
			instances = int(v)
		elif k in ("--name"):
			name = v 
		elif k in ("--args"):
			d_args = v
		elif k in ("--loglevel"):
			d_loglevel = v
		elif k in ("--chaosmonkey"):
			monkey = v
	if not input:
		print "Cannot find input files"
		sys.exit(1)
		return
	config = json_parse(open(join(input, "config.json")).read())
	resource = LlapResource(config)
	monkey_interval = int(monkey) 
	# 5% container failure every monkey_interval seconds
	monkey_percentage = 5 # 5%
	vars = {
		"home" : home,
		"version" : version,
		"instances" : instances,
		"heap" : resource.heap_size,
		"container.mb" : resource.container_size,
		"container.cores" : resource.container_cores,
		"hadoop_home" : os.getenv("HADOOP_HOME"),
		"java_home" : os.getenv("JAVA_HOME"),
		"name" : name,
		"daemon_args" : d_args,
		"daemon_loglevel" : d_loglevel,
		"monkey_interval" : monkey_interval,
		"monkey_percentage" : monkey_percentage,
		"monkey_enabled" : monkey_interval > 0
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

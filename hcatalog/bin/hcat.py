#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Resolve our absolute path
# resolve links - $0 may be a softlink

import os
import sys
import glob
import subprocess

# Determine our absolute path, resolving any symbolic links
this = os.path.realpath(sys.argv[0])
bindir = os.path.dirname(this) + os.path.sep

# Add the libexec directory to our search path so we can find the hcat-config
# module
sys.path.append(os.path.join(bindir, os.path.pardir, "libexec"))
import hcatcfg

# Find our config directory and Hadoop
hcatcfg.findCfgFile()
hcatcfg.findHadoop()

# See if any debug flags have been turned on
debug = 0
try:
  sys.argv.remove('-secretDebugCmd')
  debug = 1
except ValueError:
  pass

dumpClasspath = 0
try:
  sys.argv.remove('-classpath')
  dumpClasspath = 1
except ValueError:
  pass

# find HIVE installation directory
hcatcfg.findHive()
if 'HIVE_HOME' not in os.environ:
  sys.exit("Hive not found.  Set HIVE_HOME to directory containing Hive.")

if 'HIVE_LIB_DIR' not in os.environ:
  sys.exit("Cannot find lib dir within HIVE_HOME %s" % (os.environ['HIVE_HOME'] + os.path.sep + "lib"))

if 'HIVE_CONF_DIR' not in os.environ:
  sys.exit("Cannot find conf dir within HIVE_HOME %s" % (os.environ['HIVE_HOME'] + os.path.sep + "conf"))

##### jars addition
# find the hcatalog jar and add it to hadoop classpath
hcatPrefix = hcatcfg.findHCatPrefix(bindir)

hcatJars = glob.glob(os.path.join(hcatPrefix, 'share', 'hcatalog', 'hive-hcatalog-core-*.jar'))

if len(hcatJars) > 1:
  sys.exit("Found more than one hcatalog jar in the prefix path")

if len(hcatJars) < 1:
  sys.exit("HCatalog jar not found in directory %s" % (os.path.join(hcatPrefix, 'share', 'hcatalog', 'hive-hcatalog-core-*.jar')))

if 'HADOOP_CLASSPATH' not in os.environ:
  os.putenv('HADOOP_CLASSPATH', '')
  os.environ['HADOOP_CLASSPATH'] = ''

os.environ['HADOOP_CLASSPATH'] += os.pathsep + hcatJars[0]
# done adding the hcatalog jar to the hadoop classpath

# add all the other jars
hcatLibJarFiles = os.path.join(hcatPrefix, 'share', 'hcatalog', 'lib', '*')
os.environ['HADOOP_CLASSPATH'] += os.pathsep + hcatLibJarFiles

# adding hive jars
hiveJars = os.path.join(os.environ['HIVE_LIB_DIR'], '*')
os.environ['HADOOP_CLASSPATH'] += os.pathsep + hiveJars

##### done with addition of jars


##### add conf dirs to the classpath

# add the hive conf dir and if exists hbase conf dir

os.environ['HADOOP_CLASSPATH'] += os.pathsep + os.environ['HIVE_CONF_DIR']

# if the hbase conf dir is present in the environment, add it.
# there are no checks to see if that path exists
# FIXME add check - original shell script does not do much if the path
# does not exist either
try:
  if os.environ['HBASE_CONF_DIR'] != "":
    os.environ['HADOOP_CLASSPATH'] += os.pathsep + os.environ['HBASE_CONF_DIR']
except:
  pass

##### done with adding conf dirs to the classpath


sys.stdout.flush()

if os.name == "posix":
  hadoopcmd = "hadoop"
else:
  hadoopcmd = "hadoop.cmd"

if 'HADOOP_OPTS' not in os.environ:
  os.environ['HADOOP_OPTS'] = ''

# log under the Hive log dir but use a separate log file for HCat logs
os.environ['HADOOP_OPTS'] += " " + "-Dhive.log.file=hcat.log" + " " + "-Dhive.log.dir=" + os.path.join(os.environ['HIVE_HOME'], "logs")

##### Uncomment to debug log4j configuration
#os.environ['HADOOP_OPTS'] += " -Dlog4j.debug"

cmdLine = os.path.join(os.environ['HADOOP_PREFIX'], "bin", hadoopcmd)
if os.name == "posix":
  cmd = [cmdLine, "jar", hcatJars[0], "org.apache.hive.hcatalog.cli.HCatCli"] + sys.argv[1:len(sys.argv)]
else:
  cmd = ["call", cmdLine, "jar", hcatJars[0], "org.apache.hive.hcatalog.cli.HCatCli"] + sys.argv[1:len(sys.argv)]


if debug == 1:
  print "Would run:"
  print "exec " + str(cmd)
  print " with HADOOP_CLASSPATH set to %s" % (os.environ['HADOOP_CLASSPATH'])
  try:
    print " and HADOOP_OPTS set to %s" % (os.environ['HADOOP_OPTS'])
  except:
    pass
else:
  if dumpClasspath == 1:
    print os.environ['HADOOP_CLASSPATH']
  else:
    if os.name == "posix":
      retval = subprocess.call(cmd)
    else:
      retval = subprocess.call(cmd,  stdin=None, stdout=None, stderr=None, shell=True)
    os.environ['errorlevel'] = str(retval)
    sys.exit(retval)


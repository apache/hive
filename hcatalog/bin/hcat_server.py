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

import sys
import os
import subprocess
import time
import glob

from time import strftime

sleepTime = 3
def print_usage():
  print "Usage: %s [--config confdir] COMMAND" % (sys.argv[0])
  print "  start Start HCatalog Server"
  print "  stop Stop HCatalog Server"

def start_hcat():
  global sleepTime
  # back ground the metastore service and record the pid
  pidFile = os.path.join(os.environ['HCAT_LOG_DIR'], 'hcat.pid')

  try:
    pidFileDesc = open(pidFile, 'r')
    for line in pidFileDesc:
      pidWords = line.split()
      for pidStr in pidWords:
        pid = int(pidStr.rstrip('\n'))
    pidFileDesc.close()
  # check if service is already running, if so exit
    os.kill(pid, 0)
    sys.exit("HCatalog server appears to be running. If you are sure it is not remove %s and re-run this script" % (pidFile))
  except:
    pass

  os.environ['HIVE_SITE_XML'] = os.path.join(os.environ['HIVE_HOME'], 'conf', 'hive-site.xml')
  if os.path.exists(os.environ['HIVE_SITE_XML']) == False:
    sys.exit("Missing hive-site.xml, expected at %s" % (os.environ['HIVE_SITE_XML']))

  # Find our Warehouse dir from the config file
  #  WAREHOUSE_DIR=`sed -n '/<name>hive.metastore.warehouse.dir<\/name>/ {
  #      n
  #      s/.*<value>\(.*\)<\/value>.*/\1/p
  #      }' $HIVE_SITE_XML`
  #  HADOOP_OPTS="$HADOOP_OPTS -Dhive.metastore.warehouse.dir=$WAREHOUSE_DIR "

  # add in hive-site.xml to classpath
  if 'AUX_CLASSPATH' not in os.environ:
    os.environ['AUX_CLASSPATH'] = ''

  os.environ['AUX_CLASSPATH'] += os.pathsep + os.path.dirname(os.environ['HIVE_SITE_XML'])

  # add jars from db connectivity dir - be careful to not point to something like /lib
  try:
    for dbRootJars in glob.glob(os.path.join(os.environ['DBROOT'], '*.jar')):
      os.environ['AUX_CLASSPATH'] += os.pathsep + dbRootJars
  except:
    pass

  for hcatLibJars in glob.glob(os.path.join(os.environ['HCAT_PREFIX'], 'share', 'hcatalog', 'lib', '*.jar')):
    os.environ['AUX_CLASSPATH'] += os.pathsep + hcatLibJars

  for hcatJar in glob.glob(os.path.join(os.environ['HCAT_PREFIX'], 'share', 'hcatalog', '*.jar')):
    os.environ['AUX_CLASSPATH'] += os.pathsep + hcatJar

  if 'HADOOP_OPTS' not in os.environ:
    os.environ['HADOOP_OPTS'] = ''

  os.environ['HADOOP_OPTS'] += " -server -XX:+UseConcMarkSweepGC -XX:ErrorFile=" + os.path.join(os.environ['HCAT_LOG_DIR'], 'hcat_err_pid%p.log') + " -Xloggc:" + os.path.join(os.environ['HCAT_LOG_DIR'], 'hcat_gc.log-') + strftime("%Y%m%d%H%M") + " -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps"
  os.environ['HADOOP_HEAPSIZE'] = '2048' # 8G is better if you have it

  if os.name == "posix":
      hivecmd = "hive"
  else:
      hivecmd = "hive.cmd"

  command = os.path.join(os.environ['HIVE_HOME'], "bin", hivecmd)
  outFile = os.path.join(os.environ['HCAT_LOG_DIR'], "hcat.out")
  outfd = open(outFile, 'w')
  errFile = os.path.join(os.environ['HCAT_LOG_DIR'], "hcat.err")
  errfd = open(errFile, 'w')
  windowsTmpFile = os.path.join(os.environ['HCAT_LOG_DIR'], "windows.tmp")
  child = subprocess.Popen([command, "--service", "metastore"], stdout=outfd, stderr=errfd)
  pid = child.pid
  print "Started metastore server init, testing if initialized correctly..."
  time.sleep(sleepTime)
  try:
    if os.name == "posix":
        os.kill(pid, 0)
    else:
        ret = os.system("jps | find /I \"HiveMetaStore\" > " + windowsTmpFile + "")
        if ret != 0:
            raise Exception("error starting process")
        windowsTmpFd = open(windowsTmpFile, 'r')
        pid = int(windowsTmpFd.readline().split(" ")[0])
    pidFileDesc = open(pidFile, 'w')
    pidFileDesc.write(str(pid))
    pidFileDesc.close()
    print "Metastore initialized successfully"
  except Exception as inst:
    print inst
    sys.exit("Metastore startup failed, see %s" % (errFile))

  return

def stop_hcat():

  pidFile = os.path.join(os.environ['HCAT_LOG_DIR'], 'hcat.pid')

  pid = 0
  kill = False
  try:
    pidFileDesc = open(pidFile, 'r')
    for line in pidFileDesc:
      words = line.split()
      pid = int(words[0])

      os.kill(pid, 6)

  except:
    kill = True
    pass

  if kill == True:
    try:
      os.kill(pid, 9)
    except:
      sys.exit("Failed to stop metastore server")

  return

if __name__ == "__main__":

  this = os.path.realpath(sys.argv[0])
  bindir = os.path.dirname(this) + os.path.sep

  import hcatcfg
  hcatLogDir = hcatcfg.getHCatLogDir(bindir)
  hcatcfg.findHCatPrefix(bindir)
  os.environ['HCAT_LOG_DIR'] = hcatLogDir

  if len(sys.argv) == 1:
    print_usage()
    sys.exit()

  if sys.argv[1] == 'start':
    start_hcat()

  else:
    stop_hcat()

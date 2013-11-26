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

import os.path
import sys

# Find the config file
def findCfgFile():
    defaultConfDir = None
    if 'HCAT_PREFIX' in os.environ and os.path.exists(os.environ['HCAT_PREFIX'] + \
            os.path.sep + buildPath(["etc","hcatalog"])):
        defaultConfDir = os.environ['HCAT_PREFIX'] + os.path.sep + \
            buildPath(["etc", "hcatalog"])
    else:
        defaultConfDir = buildAbsPath(["etc", "hcatalog"])
    if 'HCAT_CONF_DIR' not in os.environ:
        os.environ['HCAT_CONF_DIR'] = defaultConfDir

def findHadoop():
    if 'HADOOP_HOME' in os.environ and os.path.exists(os.environ['HADOOP_HOME'] \
            + os.path.sep + buildPath(["bin", "hadoop"])):
        os.environ['HADOOP_PREFIX'] = os.environ['HADOOP_HOME']
    elif 'HCAT_PREFIX' in os.environ and os.path.exists(os.environ['HCAT_PREFIX'] \
            + os.path.sep + buildPath(["bin", "hadoop"])):
        os.environ['HADOOP_PREFIX'] = os.environ['HCAT_PREFIX']
    elif not ('HADOOP_PREFIX' in os.environ and \
            os.path.exists(os.environ['HADOOP_PREFIX'] + os.path.sep + \
                buildPath(["bin", "hadoop"]))):
        sys.exit("Hadoop not found.  Set HADOOP_HOME to the directory containing Hadoop.")

def concatPath(x, y):
    return x + os.path.sep + y

def buildPath(pathElements):
    return reduce(concatPath, pathElements)

def buildAbsPath(pathElements):
    return os.path.sep + buildPath(pathElements)

def findHive():
    # TODO, check for Hive in path.  For now, just look in known locations and
    # HIVE_HOME
    # No need to be OS independent checkinf for /usr/bin/hive since this is an
    # RPM specific path
    # If HIVE_HOME is set it overrides default locations
    if os.path.exists("/usr/bin/hive") and ('HIVE_HOME' not in os.environ):
        os.environ['HIVE_HOME'] = buildAbsPath(["usr", "lib", "hive"]);

    if 'HIVE_HOME' not in os.environ:
        # the api user determines how to handle the non-existence of HIVE_HOME
        return

    if os.path.exists(os.path.join(os.environ['HIVE_HOME'], 'lib')):
        os.environ['HIVE_LIB_DIR'] = os.path.join(os.environ['HIVE_HOME'], 'lib')
    else:
        return

    if os.path.exists(os.path.join(os.environ['HIVE_HOME'], 'conf')):
        os.environ['HIVE_CONF_DIR'] = os.path.join(os.environ['HIVE_HOME'], 'conf')
    else:
        return

    return

def findHCatPrefix(binDir):
    os.environ['HCAT_PREFIX'] = binDir + '..' + os.path.sep
    return os.environ['HCAT_PREFIX']

def getHCatLogDir(binDir):
    return os.path.join(binDir, '..', 'var', 'log')

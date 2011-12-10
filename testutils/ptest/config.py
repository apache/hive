# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import json
import copy
import getpass
import os.path

from Ssh import SSHConnection, SSHSet

local = None
qfile_set = None
other_set = None
remote_set = None
all_set = None

master_base_path = None
host_base_path = None
java_home = None

def load(config_file = '~/.hive_ptest.conf'):
    global local, qfile_set, other_set, remote_set, all_set
    global master_base_path, host_base_path, java_home

    config_file = os.path.expanduser(config_file)

    cfg = None
    try:
        with open(config_file) as f:
            cfg = json.loads(f.read())

        host_nodes = {}
        def get_node(host):
            if not host in host_nodes:
                host_nodes[host] = -1
            host_nodes[host] += 1
            return SSHConnection(host, host_nodes[host])

        qfile = []
        for (host, num, ) in cfg['qfile_hosts']:
            for i in range(num):
                qfile.append(get_node(host))

        other = []
        for (host, num, ) in cfg['other_hosts']:
            for i in range(num):
                other.append(get_node(host))

        local = SSHConnection('localhost')

        qfile_set = SSHSet(qfile)
        other_set = SSHSet(other)

        # Make copies, otherwise they they will be passed by reference and
        # reused.  Reuse is bad - you don't want `cd` on remote_set to affect
        # anything in the all_set.

        remote_set = SSHSet(copy.copy(qfile))
        remote_set.add(copy.copy(other))

        all_set = SSHSet(copy.copy(qfile))
        all_set.add(copy.copy(other))
        all_set.add(local)

        master_base_path = cfg['master_base_path']
        host_base_path = cfg['host_base_path'] + '-' + getpass.getuser()
        java_home = cfg['java_home']
    except Exception as e:
        raise Exception('Failed to parse your configuration file (' +
                config_file + ').  Maybe you forgot the `--config` switch?', e)

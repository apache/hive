#!/usr/bin/env python
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import sys
import os
import subprocess
from resource_management import *
from os.path import dirname
from os.path import join as join_path

class Llap(Script):
  def install(self, env):
    self.install_packages(env)
    pass

  def configure(self, env):
    import params
    env.set_params(params)

  def start(self, env):
    import params
    env.set_params(params)
    os.environ['JAVA_HOME'] = format('{java64_home}')
    # this is the same as TEZ_PREFIX
    os.environ['LLAP_DAEMON_HOME'] = format('{app_root}')
    os.environ['LLAP_DAEMON_TMP_DIR'] = format('{app_tmp_dir}')
    # this is the location where we have the llap server components (shell scripts)
    os.environ['LLAP_DAEMON_BIN_HOME'] = format('{app_root}/bin')
    # location containing llap-daemon-site.xml, tez and yarn configuration xmls as well.
    os.environ['LLAP_DAEMON_CONF_DIR'] = format("{app_root}/conf/")
    os.environ['LLAP_DAEMON_LOG_DIR'] = format("{app_log_dir}/")
    os.environ['LLAP_DAEMON_LOGGER'] = format("{app_logger}")
    os.environ['LLAP_DAEMON_LOG_LEVEL'] = format("{app_log_level}")
    os.environ['LLAP_DAEMON_HEAPSIZE'] = format("{memory_val}")
    os.environ['LLAP_DAEMON_PID_DIR'] = dirname(format("{pid_file}"))
    os.environ['LLAP_DAEMON_LD_PATH'] = format('{library_path}')
    os.environ['LLAP_DAEMON_OPTS'] = format('{daemon_args}')
    print "Debug from LLAP python script"
    print os.environ['LLAP_DAEMON_CONF_DIR']
    self.configure(env)
    location = "bash -x {app_root}/bin/llapDaemon.sh start &> {app_log_dir}/shell.out"
    process_cmd = format(location)

    subprocess.call(process_cmd, shell=True
    )

  def stop(self, env):
    import params
    env.set_params(params)

  def status(self, env):
    import params
    env.set_params(params)
    check_process_status(params.pid_file)

if __name__ == "__main__":
  Llap().execute()

# vim: tabstop=24 expandtab shiftwidth=4 softtabstop=4

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

from subprocess import Popen, PIPE, STDOUT
from shlex import split

from Buffer import Buffer

def run(cmd, quiet = False, abandon_output = True):
    proc = Popen(split(cmd), stdout = PIPE, stderr = STDOUT)
    buf = Buffer(abandon_output = abandon_output)
    line = proc.stdout.readline()
    while len(line):
        buf.put(line)
        if not quiet:
            print(line, end = '')
        line = proc.stdout.readline()
    # Process could probably close the descriptor before exiting.
    proc.wait()
    if proc.returncode != 0:
        raise Exception('Process exited with a non-zero return code.  ' +
                'Last output of the program:\n\n' +
                '---------- Start of exception log --\n' +
                buf.get_short().strip() +
                '\n---------- End of exception log --\n')
    return buf.get_long()

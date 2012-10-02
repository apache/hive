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

from threading import Thread
from Queue import Queue

import Process

class SSHConnection():
    def __init__(self, host, num = None):
        self.host = host
        if num is None:
            self.hostname = host
        else:
            self.hostname = host + '-' + str(num);
        self.pwd = '/'
        self.env = {}
        self.path = []

    def cd(self, path):
        self.pwd = path.format(host = self.hostname)

    def export(self, env, value):
        self.env[env] = value.format(host = self.hostname)

    def add_path(self, path):
        self.path.append(path.format(host = self.hostname))

    def prefix(self, cmd):
        pre = []
        pre.append('cd "{0}"'.format(self.pwd))
        for (e, v) in self.env.iteritems():
            pre.append('export {0}="{1}"'.format(e, v))
        for p in self.path:
            pre.append('export PATH="{0}:${{PATH}}"'.format(p))
        pre.append(cmd)
        return ' && '.join(pre)

    def run(self, cmd, warn_only = False, quiet = False, vewy_quiet = False,
            abandon_output = True):
        # Don't use single quotes in `cmd`, this will break and end badly.
        cmd = cmd.format(host = self.hostname)
        cmd = self.prefix(cmd)
        print(self.hostname + ' =>')
        if vewy_quiet:
            # Be vewy, vewy quiet, I'm hunting wabbits.
            print('[command hidden]\n')
            quiet = True
        else:
            print(cmd + '\n')
        cmd = '''ssh '{0}' "bash -c '{1}'"'''.format(self.host, cmd)
        try:
            return Process.run(cmd, quiet, abandon_output)
        except Exception as e:
            if warn_only:
                print(str(e) + '---------- This was only a warning, ' +
                        'it won\'t stop the execution --\n')
                return None
            else:
                raise e

class SSHSet():
    def __init__(self, conn = []):
        self.conn = conn

    def __len__(self):
        return len(self.conn)

    def add(self, conn):
        if isinstance(conn, list):
            self.conn.extend(conn)
        else:
            self.conn.append(conn)

    def cd(self, path):
        for conn in self.conn:
            conn.cd(path)

    def export(self, env, value):
        for conn in self.conn:
            conn.export(env, value)

    def add_path(self, path):
        for conn in self.conn:
            conn.add_path(path)

    def run(self, cmd, parallel = True, quiet = False, vewy_quiet = False,
            abandon_output = True, warn_only = False):
        if not parallel:
            for conn in self.conn:
                conn.run(cmd, quiet = quiet, vewy_quiet = vewy_quiet,
                        abandon_output = abandon_output, warn_only = warn_only)
        else:
            threads = []
            queue = Queue()
            def wrapper(conn, cmd, queue):
                try:
                    conn.run(cmd, quiet = quiet, vewy_quiet = vewy_quiet,
                            abandon_output = abandon_output,
                            warn_only = warn_only)
                except Exception as e:
                    queue.put(Exception(conn.hostname + ' => ' + str(e)))
            for conn in self.conn:
                thread = Thread(target = wrapper, args = (conn, cmd, queue, ))
                thread.start()
                threads.append(thread)
            for thread in threads:
                thread.join()
            if not queue.empty():
                l = []
                while not queue.empty():
                    e = queue.get()
                    l.append(str(e));
                raise Exception('\n'.join(l))

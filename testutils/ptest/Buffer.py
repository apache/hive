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

import collections

class Buffer():
    def __init__(self, size = 100, abandon_output = True):
        self.buf_short = collections.deque(maxlen = size)
        self.buf = []
        self.abandon_output = abandon_output

    def put(self, line):
        if not self.abandon_output:
            self.buf.append(line)
        self.buf_short.append(line)

    def get_short(self):
        return ''.join(self.buf_short)

    def get_long(self):
        if self.abandon_output:
            return None
        else:
            return ''.join(self.buf)

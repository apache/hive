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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
PHP_ARG_ENABLE(thrift_protocol, whether to enable the thrift_protocol extension,
[  --enable-thrift_protocol	Enable the fbthrift_protocol extension])

if test "$PHP_THRIFT_PROTOCOL" != "no"; then
  PHP_REQUIRE_CXX()
  PHP_NEW_EXTENSION(thrift_protocol, php_thrift_protocol.cpp, $ext_shared)
fi


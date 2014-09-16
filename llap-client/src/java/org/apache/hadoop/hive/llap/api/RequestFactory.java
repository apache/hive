/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.api;

/**
 * Llap server depends on QL, but QL depends on Llap. Therefore, we have a client
 * module that has interfaces, and main server/"impl" module. However, at some point QL
 * has to create a specific client impl. Currently, this is the interface used for that.
 * It's not really a client; to use Llap, you'd still need the "server" jar. In future,
 * we can move client classes (requests, readers) into this module (and remove functionality
 * from them) so that we could have a proper standalone client jar.
 */
public interface RequestFactory {
  public Request createLocalRequest();
}

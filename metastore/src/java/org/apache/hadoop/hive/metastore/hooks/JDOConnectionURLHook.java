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

package org.apache.hadoop.hive.metastore.hooks;

import org.apache.hadoop.conf.Configuration;

/**
 * JDOConnectURLHook is used to get the URL that JDO uses to connect to the
 * database that stores the metastore data. Classes implementing this must be
 * thread-safe (for Thrift server).
 */
public interface JDOConnectionURLHook {

  /**
   * Gets the connection URL to supply to JDO. In addition to initialization,
   * this method will be called after a connection failure for each reconnect
   * attempt.
   *
   * @param conf The configuration used to initialize this instance of the HMS
   * @return
   * @throws Exception
   */
  public String getJdoConnectionUrl(Configuration conf)
  throws Exception;

  /**
   * Alerts this that the connection URL was bad. Can be used to collect stats,
   * etc.
   *
   * @param url
   */
  public void notifyBadConnectionUrl(String url);
}

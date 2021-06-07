/*
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

package org.apache.hadoop.hive.ql.session;

import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;

public class SessionStateUtil {

  private SessionStateUtil() {

  }

  /**
   * @param conf Configuration object used for getting the query state, should contain the query id
   * @param key The resource identifier
   * @return The requested resource, or an empty Optional if either the SessionState, QueryState or the resource itself
   * could not be found
   */
  public static Optional<Object> getResource(Configuration conf, String key) {
    return getQueryState(conf).map(state -> state.getResource(key));
  }

  /**
   * @param conf Configuration object used for getting the query state, should contain the query id
   * @param key The resource identifier
   * @return The requested string property, or an empty Optional if either the SessionState, QueryState or the
   * resource itself could not be found, or the resource is not of type String
   */
  public static Optional<String> getProperty(Configuration conf, String key) {
    return getResource(conf, key).filter(o -> o instanceof String).map(o -> (String) o);
  }

  /**
   * @param conf Configuration object used for getting the query state, should contain the query id
   * @param key The resource identifier
   * @param resource The resource to save into the QueryState
   * @return whether operation succeeded
   */
  public static boolean addResource(Configuration conf, String key, Object resource) {
    Optional<QueryState> queryState = getQueryState(conf);
    if (queryState.isPresent()) {
      queryState.get().addResource(key, resource);
      return true;
    } else {
      return false;
    }
  }

  private static Optional<QueryState> getQueryState(Configuration conf) {
    return Optional.ofNullable(SessionState.get())
        .map(session -> session.getQueryState(conf.get(HiveConf.ConfVars.HIVEQUERYID.varname)));
  }
}
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

import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;


/**
 * Collection of helper methods to get resources from the {@link QueryState} associated with the active session.
 */
public class SessionStateUtil {

  /**
   * Get a resource with a specific resource identifier stored on the SessionState.
   * @param configuration a Hadoop configuration
   * @param resourceId identifier of the resource
   * @return resource object, can be null
   */
  @Nullable
  public static Object getResourceFromSessionState(Configuration configuration, String resourceId) {
    if (hasResourceInSessionState(configuration,resourceId)) {
      QueryState queryState =
          SessionState.get().getQueryState(configuration.get(HiveConf.ConfVars.HIVEQUERYID.varname));
      return queryState.getResource(resourceId);
    }
    return null;
  }

  /**
   * Store a resource in the SessionState.
   * @param configuration a Hadoop configuration
   * @param resourceId identifier of the resource
   * @param resource resource object
   * @return true, if the operation was successful
   */
  public static boolean addResourceToSessionState(Configuration configuration, String resourceId, Object resource) {
    if (hasQueryStateInSessionState(configuration)) {
      SessionState.get().getQueryState(configuration.get(HiveConf.ConfVars.HIVEQUERYID.varname))
          .addResource(resourceId, resource);
      return true;
    }
    return false;
  }

  /**
   * Check for the existence of a resource in the SessionState.
   * @param configuration a Hadoop configuration
   * @param resourceId identifier of the resource
   * @return true, if the resource can be found
   */
  private static boolean hasResourceInSessionState(Configuration configuration, String resourceId) {
    if (hasQueryStateInSessionState(configuration)) {
      Object resource = SessionState.get().getQueryState(configuration.get(HiveConf.ConfVars.HIVEQUERYID.varname))
          .getResource(resourceId);
      if (resource != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check fo the existence of QueryState object in the SessionState.
   * @param configuration a Hadoop configuration
   * @return true, if the QueryState can be found
   */
  private static boolean hasQueryStateInSessionState(Configuration configuration) {
    if (SessionState.get() != null) {
      QueryState queryState =
          SessionState.get().getQueryState(configuration.get(HiveConf.ConfVars.HIVEQUERYID.varname));
      if (queryState != null) {
        return true;
      }
    }
    return false;
  }

}

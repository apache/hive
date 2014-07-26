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

package org.apache.hadoop.hive.conf;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;

/**
 * Hive Configuration utils
 */
@Private
public class HiveConfUtil {
  /**
   * Check if metastore is being used in embedded mode.
   * This utility function exists so that the logic for determining the mode is same
   * in HiveConf and HiveMetaStoreClient
   * @param msUri - metastore server uri
   * @return
   */
  public static boolean isEmbeddedMetaStore(String msUri) {
    return (msUri == null) ? true : msUri.trim().isEmpty();
  }
}

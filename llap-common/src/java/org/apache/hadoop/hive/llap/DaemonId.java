/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap;

import java.util.UUID;

public class DaemonId {
  private final String userName;
  private final String clusterName;
  private final String appId;
  private final String hostName;
  private final long startTimeMs;
  private final String uuidString;

  public DaemonId(
      String userName, String clusterName, String hostName, String appId, long startTime) {
    this.userName = userName;
    this.clusterName = clusterName;
    this.appId = appId;
    this.hostName = hostName;
    this.startTimeMs = startTime;
    this.uuidString = UUID.randomUUID().toString();
  }

  public String getClusterString() {
    return createClusterString(userName, clusterName);
  }

  public static String createClusterString(String userName, String clusterName) {
    // Note that this doesn't include appId. We assume that all the subsequent instances
    // of the same user+cluster are logically the same, i.e. all the ZK paths will be reused,
    // all the security tokens/etc. should transition between them, etc.
    return userName + "_" + clusterName;
  }

  public String getApplicationId() {
    return appId;
  }

  public String getUniqueNodeIdInCluster() {
    return hostName + "_" + startTimeMs + "_" + uuidString;
  }
}
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

public class DaemonId {
  private final String userName;
  private final String clusterName;
  private final String appId;
  private final String hostName;
  private final long startTime;

  public DaemonId(String userName, String clusterName, String hostName, String appId,
      long startTime) {
    this.userName = userName;
    this.clusterName = clusterName;
    this.appId = appId;
    this.hostName = hostName;
    this.startTime = startTime;
    // TODO: we could also get an unique number per daemon.
  }

  public String getClusterString() {
    return userName + "_" + clusterName + "_" + appId;
  }

  public String getApplicationId() {
    return appId;
  }
}
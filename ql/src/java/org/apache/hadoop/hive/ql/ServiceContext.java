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
package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.function.Supplier;

/**
 * Information context about the service, most probably HiveServer2.
 */
public class ServiceContext {

  private final String clusterId;
  private final Supplier<String> host;
  private final Supplier<Integer> port;

  public ServiceContext(Supplier<String> host, Supplier<Integer> port) {
    this.host = host;
    this.port = port;
    this.clusterId = findClusterId();
  }

  /**
   * Logic for finding cluster id if any. Default value is empty string instead of null to stay safe
   * with HiveConf.set().
   * Precedence order: cli opt, env var, empty string
   * Can be used as a utility.
   *
   * @return cluster id found from environment of system props
   */
  public static String findClusterId() {
    return System.getProperty(Constants.CLUSTER_ID_CLI_OPT_NAME,
        getClusterIdFromEnv());
  }

  private static String getClusterIdFromEnv() {
    return System.getenv(Constants.CLUSTER_ID_ENV_VAR_NAME) == null ? "" : System.getenv(
        Constants.CLUSTER_ID_ENV_VAR_NAME);
  }

  public void setClusterIdInConf(HiveConf hiveConf) {
    hiveConf.set(Constants.CLUSTER_ID_HIVE_CONF_PROP, clusterId);
  }

  public String getHost() {
    return host.get();
  }

  public int getPort() {
    return port.get();
  }

  public String getClusterId() {
    return clusterId;
  }
}

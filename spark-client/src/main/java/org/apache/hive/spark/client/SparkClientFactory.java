/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.spark.client.rpc.RpcServer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Factory for SparkClient instances.
 */
@InterfaceAudience.Private
public final class SparkClientFactory {

  /** Used by client and driver to share a client ID for establishing an RPC session. */
  static final String CONF_CLIENT_ID = "spark.client.authentication.client_id";

  /** Used by client and driver to share a secret for establishing an RPC session. */
  static final String CONF_KEY_SECRET = "spark.client.authentication.secret";

  private static volatile RpcServer server = null;
  private static final Object serverLock = new Object();

  /**
   * Initializes the SparkClient library. Must be called before creating client instances.
   *
   * @param conf Map containing configuration parameters for the client library.
   */
  public static void initialize(Map<String, String> conf) throws IOException {
    if (server == null) {
      synchronized (serverLock) {
        if (server == null) {
          try {
            server = new RpcServer(conf);
          } catch (InterruptedException ie) {
            throw Throwables.propagate(ie);
          }
        }
      }
    }
  }

  /** Stops the SparkClient library. */
  public static void stop() {
    if (server != null) {
      synchronized (serverLock) {
        if (server != null) {
          server.close();
          server = null;
        }
      }
    }
  }

  /**
   * Instantiates a new Spark client.
   *
   * @param sparkConf Configuration for the remote Spark application, contains spark.* properties.
   * @param hiveConf Configuration for Hive, contains hive.* properties.
   */
  public static SparkClient createClient(Map<String, String> sparkConf, HiveConf hiveConf,
                                         String sessionId) throws IOException {
    Preconditions.checkState(server != null,
            "Invalid state: Hive on Spark RPC Server has not been initialized");
    switch (hiveConf.getVar(HiveConf.ConfVars.SPARK_CLIENT_TYPE)) {
    case HiveConf.HIVE_SPARK_SUBMIT_CLIENT:
      return new SparkSubmitSparkClient(server, sparkConf, hiveConf, sessionId);
    case HiveConf.HIVE_SPARK_LAUNCHER_CLIENT:
      return new SparkLauncherSparkClient(server, sparkConf, hiveConf, sessionId);
    default:
      throw new IllegalArgumentException("Unknown Hive on Spark launcher type " + hiveConf.getVar(
              HiveConf.ConfVars.SPARK_CLIENT_TYPE) + " valid options are " +
              HiveConf.HIVE_SPARK_SUBMIT_CLIENT + " or " + HiveConf.HIVE_SPARK_LAUNCHER_CLIENT);
    }
  }
}

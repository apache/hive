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
import org.apache.spark.SparkException;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Factory for SparkClient instances.
 */
@InterfaceAudience.Private
public final class SparkClientFactory {

  /** Used to run the driver in-process, mostly for testing. */
  static final String CONF_KEY_IN_PROCESS = "spark.client.do_not_use.run_driver_in_process";

  /** Used by client and driver to share a client ID for establishing an RPC session. */
  static final String CONF_CLIENT_ID = "spark.client.authentication.client_id";

  /** Used by client and driver to share a secret for establishing an RPC session. */
  static final String CONF_KEY_SECRET = "spark.client.authentication.secret";

  private static RpcServer server = null;

  /**
   * Initializes the SparkClient library. Must be called before creating client instances.
   *
   * @param conf Map containing configuration parameters for the client library.
   */
  public static synchronized void initialize(Map<String, String> conf) throws IOException {
    if (server == null) {
      try {
        server = new RpcServer(conf);
      } catch (InterruptedException ie) {
        throw Throwables.propagate(ie);
      }
    }
  }

  /** Stops the SparkClient library. */
  public static synchronized void stop() {
    if (server != null) {
      server.close();
      server = null;
    }
  }

  /**
   * Instantiates a new Spark client.
   *
   * @param sparkConf Configuration for the remote Spark application, contains spark.* properties.
   * @param hiveConf Configuration for Hive, contains hive.* properties.
   */
  public static synchronized SparkClient createClient(Map<String, String> sparkConf, HiveConf hiveConf)
      throws IOException, SparkException {
    Preconditions.checkState(server != null, "initialize() not called.");
    return new SparkClientImpl(server, sparkConf, hiveConf);
  }

}

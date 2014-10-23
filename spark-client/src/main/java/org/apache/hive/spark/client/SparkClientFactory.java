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

import akka.actor.ActorSystem;
import com.google.common.collect.Maps;
import org.apache.spark.SparkException;

/**
 * Factory for SparkClient instances.
 */
public final class SparkClientFactory {

  static ActorSystem actorSystem = null;
  static String akkaUrl = null;
  static String secret = null;

  private static boolean initialized = false;

  /**
   * Initializes the SparkClient library. Must be called before creating client instances.
   *
   * @param conf Map containing configuration parameters for the client.
   */
  public static synchronized void initialize(Map<String, String> conf) throws IOException {
    secret = akka.util.Crypt.generateSecureCookie();

    Map<String, String> akkaConf = Maps.newHashMap(conf);
    akkaConf.put(ClientUtils.CONF_KEY_SECRET, secret);

    ClientUtils.ActorSystemInfo info = ClientUtils.createActorSystem(akkaConf);
    actorSystem = info.system;
    akkaUrl = info.url;
    initialized = true;
  }

  /** Stops the SparkClient library. */
  public static synchronized void stop() {
    if (initialized) {
      actorSystem.shutdown();
      actorSystem = null;
      akkaUrl = null;
      secret = null;
      initialized = false;
    }
  }

  /**
   * Instantiates a new Spark client.
   *
   * @param conf Configuration for the remote Spark application.
   */
  public static synchronized SparkClient createClient(Map<String, String> conf)
      throws IOException, SparkException {
    if (!initialized) {
      throw new IllegalStateException("Library is not initialized. Call initialize() first.");
    }
    return new SparkClientImpl(conf);
  }

}

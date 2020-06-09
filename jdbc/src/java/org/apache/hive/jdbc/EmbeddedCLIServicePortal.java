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

package org.apache.hive.jdbc;

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCLIService.Iface;

public class EmbeddedCLIServicePortal {

  public static Iface get(Map<String, String> hiveConfs) {
    TCLIService.Iface embeddedClient;
    try {
      Class<TCLIService.Iface> clazz =
          (Class<Iface>) Class.forName("org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService");
      embeddedClient = clazz.newInstance();
      ((Service) embeddedClient).init(buildOverlayedConf(hiveConfs));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Please Load hive-service jar to the classpath to enable embedded mode");
    } catch (Exception e) {
      throw new RuntimeException("Error initializing embedded mode", e);
    }
    return embeddedClient;
  }

  private static HiveConf buildOverlayedConf(Map<String, String> confOverlay) {
    HiveConf conf = new HiveConf();
    if (confOverlay != null && !confOverlay.isEmpty()) {
      // apply overlay query specific settings, if any
      for (Map.Entry<String, String> confEntry : confOverlay.entrySet()) {
        try {
          conf.set(confEntry.getKey(), confEntry.getValue());
        } catch (IllegalArgumentException e) {
          throw new RuntimeException("Error applying statement specific settings", e);
        }
      }
    }
    return conf;
  }

}

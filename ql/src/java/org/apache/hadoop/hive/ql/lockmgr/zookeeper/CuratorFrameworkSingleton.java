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

package org.apache.hadoop.hive.ql.lockmgr.zookeeper;


import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hive.conf.HiveConf;

public class CuratorFrameworkSingleton {
  private static HiveConf conf = null;
  private static CuratorFramework sharedClient = null;
  static final Logger LOG = LoggerFactory.getLogger("CuratorFrameworkSingleton");
  static {
    // Add shutdown hook.
    ShutdownHookManager.addShutdownHook(new Runnable() {
      @Override
      public void run() {
        closeAndReleaseInstance();
      }
    });
  }

  public static synchronized CuratorFramework getInstance(HiveConf hiveConf) {
    if (sharedClient == null) {
      // Create a client instance
      if (hiveConf == null) {
        conf = new HiveConf();
      } else {
        conf = hiveConf;
      }

      sharedClient = conf.getZKConfig().getNewZookeeperClient();
      sharedClient.start();
    }

    return sharedClient;
  }

  public static synchronized void closeAndReleaseInstance() {
    if (sharedClient != null) {
      sharedClient.close();
      sharedClient = null;
      String shutdownMsg = "Closing ZooKeeper client.";
      LOG.info(shutdownMsg);
    }
  }
}

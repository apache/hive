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

package org.apache.hadoop.hive.ql.util;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

public class ZooKeeperHiveHelper {
  public static final Log LOG = LogFactory.getLog(ZooKeeperHiveHelper.class.getName());
  public static final String ZOOKEEPER_PATH_SEPARATOR = "/";
  /**
   * Get the ensemble server addresses from the configuration. The format is: host1:port,
   * host2:port..
   *
   * @param conf
   **/
  public static String getQuorumServers(HiveConf conf) {
    String[] hosts = conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM).split(",");
    String port = conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT);
    StringBuilder quorum = new StringBuilder();
    for (int i = 0; i < hosts.length; i++) {
      quorum.append(hosts[i].trim());
      if (!hosts[i].contains(":")) {
        // if the hostname doesn't contain a port, add the configured port to hostname
        quorum.append(":");
        quorum.append(port);
      }

      if (i != hosts.length - 1)
        quorum.append(",");
    }

    return quorum.toString();
  }


  /**
   * Create a path on ZooKeeper, if it does not already exist ("mkdir -p")
   *
   * @param zooKeeperClient ZooKeeper session
   * @param path string with ZOOKEEPER_PATH_SEPARATOR as the separator
   * @param acl list of ACL entries
   * @param createMode for create mode of each node in the patch
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static String createPathRecursively(ZooKeeper zooKeeperClient, String path, List<ACL> acl,
      CreateMode createMode) throws KeeperException, InterruptedException {
    String[] pathComponents = StringUtils.splitByWholeSeparator(path, ZOOKEEPER_PATH_SEPARATOR);
    String currentPath = "";
    for (String pathComponent : pathComponents) {
      currentPath += ZOOKEEPER_PATH_SEPARATOR + pathComponent;
      try {
        String node = zooKeeperClient.create(currentPath, new byte[0], acl, createMode);
        LOG.info("Created path: " + node);
      } catch (KeeperException.NodeExistsException e) {
        // Do nothing here
      }
    }
    return currentPath;
  }

  /**
   * A no-op watcher class
   */
  public static class DummyWatcher implements Watcher {
    public void process(org.apache.zookeeper.WatchedEvent event) {
    }
  }

}

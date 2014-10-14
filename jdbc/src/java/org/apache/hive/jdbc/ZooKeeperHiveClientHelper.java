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

package org.apache.hive.jdbc;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperHiveClientHelper {
  public static final Log LOG = LogFactory.getLog(ZooKeeperHiveClientHelper.class.getName());

  /**
   * A no-op watcher class
   */
  public static class DummyWatcher implements Watcher {
    public void process(org.apache.zookeeper.WatchedEvent event) {
    }
  }

  /**
   * Resolve to a host:port by connecting to ZooKeeper and picking a host randomly.
   *
   * @param uri
   * @param connParams
   * @return
   * @throws SQLException
   */
  static String getNextServerUriFromZooKeeper(JdbcConnectionParams connParams)
      throws ZooKeeperHiveClientException {
    String zooKeeperEnsemble = connParams.getZooKeeperEnsemble();
    String zooKeeperNamespace =
        connParams.getSessionVars().get(JdbcConnectionParams.ZOOKEEPER_NAMESPACE);
    if ((zooKeeperNamespace == null) || (zooKeeperNamespace.isEmpty())) {
      zooKeeperNamespace = JdbcConnectionParams.ZOOKEEPER_DEFAULT_NAMESPACE;
    }
    List<String> serverHosts;
    Random randomizer = new Random();
    String serverNode;
    // Pick a random HiveServer2 host from the ZooKeeper namspace
    try {
      ZooKeeper zooKeeperClient =
          new ZooKeeper(zooKeeperEnsemble, JdbcConnectionParams.ZOOKEEPER_SESSION_TIMEOUT,
              new ZooKeeperHiveClientHelper.DummyWatcher());
      // All the HiveServer2 host nodes that are in ZooKeeper currently
      serverHosts = zooKeeperClient.getChildren("/" + zooKeeperNamespace, false);
      // Remove the znodes we've already tried from this list
      serverHosts.removeAll(connParams.getRejectedHostZnodePaths());
      if (serverHosts.isEmpty()) {
        throw new ZooKeeperHiveClientException(
            "Tried all existing HiveServer2 uris from ZooKeeper.");
      }
      // Now pick a host randomly
      serverNode = serverHosts.get(randomizer.nextInt(serverHosts.size()));
      connParams.setCurrentHostZnodePath(serverNode);
      // Read the value from the node (UTF-8 enoded byte array) and convert it to a String
      String serverUri =
          new String(zooKeeperClient.getData("/" + zooKeeperNamespace + "/" + serverNode, false,
              null), Charset.forName("UTF-8"));
      LOG.info("Selected HiveServer2 instance with uri: " + serverUri);
      return serverUri;
    } catch (Exception e) {
      throw new ZooKeeperHiveClientException("Unable to read HiveServer2 uri from ZooKeeper", e);
    }
  }

}

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

package org.apache.hadoop.hive.common;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The watcher class which sets the de-register flag when the given znode is deleted.
 */
public class ZKDeRegisterWatcher implements Watcher {
  private static final Logger LOG = LoggerFactory.getLogger(ZKDeRegisterWatcher.class);
  private final ZooKeeperHiveHelper zooKeeperHiveHelper;
  private String zNodePath;
  private boolean needToSetAgain;

  public ZKDeRegisterWatcher(ZooKeeperHiveHelper zooKeeperHiveHelper) {
    this.zooKeeperHiveHelper = zooKeeperHiveHelper;
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getType().equals(Event.EventType.NodeDeleted)) {
      zooKeeperHiveHelper.deregisterZnode();
    } else {
      if (Event.KeeperState.SyncConnected == event.getState() && Event.EventType.None == event.getType()) {
        needToSetAgain = true; // connection reestablishment
      } else if (needToSetAgain &&
          zNodePath != null &&
          Event.KeeperState.SyncConnected == event.getState() &&
          Event.EventType.NodeDataChanged == event.getType() &&
          zNodePath.equals(event.getPath())) {
        LOG.warn("DeRegisterWatcher is called with WatchedEvent[{}] other than NodeDeleted event. "
            + "DeRegisterWatcher will be registered again.", event.toString());
        try {
          zooKeeperHiveHelper.setDeRegisterWatcher(this);
          needToSetAgain = false;
        } catch (Exception e) {
          LOG.error("It's failed to register DeRegisterWatcher again for this HiveServer2 instance on ZooKeeper.", e);
        }
      } else {
        LOG.info("DeRegisterWatcher is called with WatchedEvent[{}] which server doesn't expect.", event.toString());
      }
    }
  }

  public void setZNodePath(String zNodePath) {
    this.zNodePath = zNodePath;
  }

  public String getZNodePath() {
    return zNodePath;
  }
}

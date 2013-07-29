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

package org.apache.hadoop.hive.ql.lockmgr.zookeeper;

import static org.mockito.Mockito.*;

import java.util.Collections;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;

public class TestZookeeperLockManager {

  private static final Joiner SLASH = Joiner.on("/");
  private static final String PARENT = "hive";
  private static final String TABLE = "t1";
  private static final String PARENT_LOCK_PATH = SLASH.join("", PARENT, TABLE);
  private static final String TABLE_LOCK_PATH =  SLASH.join("", PARENT, TABLE, "00001");
  private HiveConf conf;
  private ZooKeeper zooKeeper;
  private HiveLockObject hiveLock;
  private ZooKeeperHiveLock zLock;

  @Before
  public void setup() {
    conf = new HiveConf();
    zooKeeper = mock(ZooKeeper.class);
    hiveLock = mock(HiveLockObject.class);
    when(hiveLock.getName()).thenReturn(TABLE);
    zLock = new ZooKeeperHiveLock(TABLE_LOCK_PATH, hiveLock, HiveLockMode.SHARED);
  }

  @Test
  public void testDeleteNoChildren() throws Exception {
    ZooKeeperHiveLockManager.unlockPrimitive(conf, zooKeeper, zLock, PARENT);
    verify(zooKeeper).delete(TABLE_LOCK_PATH, -1);
    verify(zooKeeper).getChildren(PARENT_LOCK_PATH, false);
    verify(zooKeeper).delete(PARENT_LOCK_PATH, -1);
    verifyNoMoreInteractions(zooKeeper);
  }
  /**
   * Tests two threads racing to delete PARENT_LOCK_PATH
   */
  @Test
  public void testDeleteNoChildrenNodeDoesNotExist() throws Exception {
    doThrow(new KeeperException.NoNodeException()).when(zooKeeper).delete(PARENT_LOCK_PATH, -1);
    ZooKeeperHiveLockManager.unlockPrimitive(conf, zooKeeper, zLock, PARENT);
    verify(zooKeeper).delete(TABLE_LOCK_PATH, -1);
    verify(zooKeeper).getChildren(PARENT_LOCK_PATH, false);
    verify(zooKeeper).delete(PARENT_LOCK_PATH, -1);
    verifyNoMoreInteractions(zooKeeper);
  }
  @Test
  public void testDeleteWithChildren() throws Exception {
    when(zooKeeper.getChildren(PARENT_LOCK_PATH, false)).thenReturn(Collections.singletonList("somechild"));
    ZooKeeperHiveLockManager.unlockPrimitive(conf, zooKeeper, zLock, PARENT);
    verify(zooKeeper).delete(TABLE_LOCK_PATH, -1);
    verify(zooKeeper).getChildren(PARENT_LOCK_PATH, false);
    verifyNoMoreInteractions(zooKeeper);
  }
}

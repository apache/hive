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

package org.apache.hadoop.hive.ql.lockmgr;



import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.junit.Assert;
import org.junit.Test;

/**
 * TestEmbeddedLockManager.
 *
 */
public class TestEmbeddedLockManager {

  private int counter;
  private HiveConf conf = new HiveConf();

  @Test
  public void testLocking() throws LockException {
    HiveConf conf = new HiveConf();
    conf.set("hive.lock.numretries", "0");
    conf.set("hive.unlock.numretries", "0");
    EmbeddedLockManager manager = new EmbeddedLockManager();
    manager.setContext(new HiveLockManagerCtx(conf));

    String path1 = "database1/table1/x=100";
    String path2 = "database1/table1/x=200";
    String path3 = "database1/table2";
    String path4 = "database2";

    HiveLockObject path1sel1 = lockObj(path1, "select");
    HiveLock path1sel1Lock = manager.lock(path1sel1, HiveLockMode.SHARED, false);
    Assert.assertNotNull(path1sel1Lock);
    Assert.assertEquals(1, manager.getLocks(path1sel1, false, false).size());
    Assert.assertEquals(1, manager.getLocks(path1sel1, false, true).size());
    Assert.assertEquals(1, manager.getLocks(false, true).size());

    HiveLockObject path1up1 = lockObj(path1, "update");
    Assert.assertNull(manager.lock(path1up1, HiveLockMode.EXCLUSIVE, false));

    HiveLockObject path1sel2 = lockObj(path1, "select");
    HiveLock path1sel2Lock = manager.lock(path1sel2, HiveLockMode.SHARED, false);
    Assert.assertNotNull(path1sel2Lock);
    Assert.assertEquals(1, manager.getLocks(path1sel1, false, false).size());
    Assert.assertEquals(2, manager.getLocks(path1sel1, false, true).size());
    Assert.assertEquals(2, manager.getLocks(path1sel2, false, true).size());
    Assert.assertEquals(2, manager.getLocks(false, true).size());

    HiveLockObject path1up2 = lockObj(path1, "update");
    Assert.assertNull(manager.lock(path1up2, HiveLockMode.EXCLUSIVE, false));

    HiveLockObject path2sel1 = lockObj(path2, "select");
    HiveLock path2sel1Lock = manager.lock(path2sel1, HiveLockMode.SHARED, false);
    Assert.assertNotNull(path2sel1Lock);
    Assert.assertEquals(1, manager.getLocks(path1sel1, false, false).size());
    Assert.assertEquals(2, manager.getLocks(path1sel1, false, true).size());
    Assert.assertEquals(2, manager.getLocks(path1sel2, false, true).size());
    Assert.assertEquals(1, manager.getLocks(path2sel1, false, true).size());
    Assert.assertEquals(3, manager.getLocks(false, true).size());

    HiveLockObject path3sel = lockObj(path3, "select");
    HiveLock path3selLock = manager.lock(path3sel, HiveLockMode.SHARED, false);
    Assert.assertNotNull(path3selLock);
    Assert.assertEquals(1, manager.getLocks(path1sel1, false, false).size());
    Assert.assertEquals(2, manager.getLocks(path1sel1, false, true).size());
    Assert.assertEquals(2, manager.getLocks(path1sel2, false, true).size());
    Assert.assertEquals(1, manager.getLocks(path2sel1, false, true).size());
    Assert.assertEquals(1, manager.getLocks(path3sel, false, true).size());
    Assert.assertEquals(4, manager.getLocks(false, true).size());

    manager.unlock(path1sel1Lock);
    Assert.assertEquals(1, manager.getLocks(path1sel1, false, false).size());
    Assert.assertEquals(1, manager.getLocks(path1sel1, false, true).size());
    Assert.assertEquals(1, manager.getLocks(path1sel2, false, true).size());
    Assert.assertEquals(1, manager.getLocks(path2sel1, false, true).size());
    Assert.assertEquals(1, manager.getLocks(path3sel, false, true).size());
    Assert.assertEquals(3, manager.getLocks(false, true).size());

    manager.unlock(path1sel2Lock);
    Assert.assertEquals(1, manager.getLocks(path2sel1, false, true).size());
    Assert.assertEquals(1, manager.getLocks(path3sel, false, true).size());
    Assert.assertEquals(2, manager.getLocks(false, true).size());

    manager.unlock(path2sel1Lock);
    Assert.assertEquals(1, manager.getLocks(path3sel, false, true).size());
    Assert.assertEquals(1, manager.getLocks(false, true).size());

    manager.unlock(path3selLock);
    Assert.assertEquals(0, manager.getLocks(false, true).size());

    HiveLockObject path2up1 = lockObj(path2, "update");
    HiveLock path2up1Lock = manager.lock(path2up1, HiveLockMode.EXCLUSIVE, false);
    Assert.assertNotNull(path2up1Lock);
    Assert.assertEquals(1, manager.getLocks(path2up1, false, true).size());
    Assert.assertEquals(1, manager.getLocks(false, true).size());

    Assert.assertNull(manager.lock(path2up1, HiveLockMode.EXCLUSIVE, false));

    HiveLockObject path1sel3 = lockObj(path1, "select");
    HiveLockObject path2sel2 = lockObj(path2, "select");
    Assert.assertNotNull(manager.lock(path1sel3, HiveLockMode.SHARED, false));
    Assert.assertNull(manager.lock(path2sel2, HiveLockMode.SHARED, false));

    Assert.assertEquals(1, manager.getLocks(path2up1, false, true).size());
    Assert.assertEquals(1, manager.getLocks(path1sel3, false, true).size());
    Assert.assertEquals(2, manager.getLocks(false, true).size());
  }

  private HiveLockObject lockObj(String path, String query) {
    HiveLockObjectData data = new HiveLockObjectData(String.valueOf(++counter), null, null,
        query, conf);
    return new HiveLockObject(path.split("/"), data);
  }
}

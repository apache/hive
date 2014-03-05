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

package org.apache.hadoop.hive.ql.lockmgr;

import junit.framework.Assert;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TestDummyTxnManager {

  @Test
  public void testDedupLockObjects() {
    List<HiveLockObj> lockObjs = new ArrayList<HiveLockObj>();
    String path1 = "path1";
    String path2 = "path2";
    HiveLockObjectData lockData1 = new HiveLockObjectData(
        "query1", "1", "IMPLICIT", "drop table table1");
    HiveLockObjectData lockData2 = new HiveLockObjectData(
        "query1", "1", "IMPLICIT", "drop table table1");

    // Start with the following locks:
    // [path1, shared]
    // [path1, exclusive]
    // [path2, shared]
    // [path2, shared]
    // [path2, shared]
    lockObjs.add(new HiveLockObj(new HiveLockObject(path1, lockData1), HiveLockMode.SHARED));
    String name1 = lockObjs.get(lockObjs.size() - 1).getName();
    lockObjs.add(new HiveLockObj(new HiveLockObject(path1, lockData1), HiveLockMode.EXCLUSIVE));
    lockObjs.add(new HiveLockObj(new HiveLockObject(path2, lockData2), HiveLockMode.SHARED));
    String name2 = lockObjs.get(lockObjs.size() - 1).getName();
    lockObjs.add(new HiveLockObj(new HiveLockObject(path2, lockData2), HiveLockMode.SHARED));
    lockObjs.add(new HiveLockObj(new HiveLockObject(path2, lockData2), HiveLockMode.SHARED));

    DummyTxnManager.dedupLockObjects(lockObjs);

    // After dedup we should be left with 2 locks:
    // [path1, exclusive]
    // [path2, shared]
    Assert.assertEquals("Locks should be deduped", 2, lockObjs.size());

    Comparator<HiveLockObj> cmp = new Comparator<HiveLockObj>() {
      public int compare(HiveLockObj lock1, HiveLockObj lock2) {
        return lock1.getName().compareTo(lock2.getName());
      }
    };
    Collections.sort(lockObjs, cmp);

    HiveLockObj lockObj = lockObjs.get(0);
    Assert.assertEquals(name1, lockObj.getName());
    Assert.assertEquals(HiveLockMode.EXCLUSIVE, lockObj.getMode());

    lockObj = lockObjs.get(1);
    Assert.assertEquals(name2, lockObj.getName());
    Assert.assertEquals(HiveLockMode.SHARED, lockObj.getMode());
  }
}

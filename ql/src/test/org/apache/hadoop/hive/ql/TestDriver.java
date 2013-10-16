package org.apache.hadoop.hive.ql;

import java.util.*;
import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObj;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;

public class TestDriver extends TestCase {
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

    Driver.dedupLockObjects(lockObjs);

    // After dedup we should be left with 2 locks:
    // [path1, exclusive]
    // [path2, shared]
    assertEquals("Locks should be deduped", 2, lockObjs.size());

    Comparator<HiveLockObj> cmp = new Comparator<HiveLockObj>() {
      public int compare(HiveLockObj lock1, HiveLockObj lock2) {
        return lock1.getName().compareTo(lock2.getName());
      }
    };
    Collections.sort(lockObjs, cmp);

    HiveLockObj lockObj = lockObjs.get(0);
    assertEquals(name1, lockObj.getName());
    assertEquals(HiveLockMode.EXCLUSIVE, lockObj.getMode());

    lockObj = lockObjs.get(1);
    assertEquals(name2, lockObj.getName());
    assertEquals(HiveLockMode.SHARED, lockObj.getMode());
  }
}

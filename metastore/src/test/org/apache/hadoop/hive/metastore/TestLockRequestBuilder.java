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
package org.apache.hadoop.hive.metastore;

import junit.framework.Assert;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.junit.Test;

import java.net.InetAddress;
import java.util.List;

/**
 * Tests for LockRequestBuilder.
 */
public class TestLockRequestBuilder {

  // Test failure if user not set
  @Test
  public void noUser() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp);
    boolean caughtException = false;
    try {
      LockRequest req = bldr.build();
    } catch (RuntimeException e) {
      Assert.assertEquals("Cannot build a lock without giving a user", e.getMessage());
      caughtException = true;
    }
    Assert.assertTrue(caughtException);
  }

  // Test that database and table don't coalesce.
  @Test
  public void testDbTable() throws Exception {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp).setUser("fred");
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp);
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(2, locks.size());
    Assert.assertEquals("fred", req.getUser());
    Assert.assertEquals(InetAddress.getLocalHost().getHostName(), req.getHostname());
  }

  // Test that database and table don't coalesce.
  @Test
  public void testTablePartition() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp).setUser(null);
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    bldr.addLockComponent(comp);
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(2, locks.size());
    Assert.assertEquals("unknown", req.getUser());
  }

  // Test that 2 separate databases don't coalesce.
  @Test
  public void testTwoSeparateDbs() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp).setUser("fred");
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "yourdb");
    bldr.addLockComponent(comp);
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(2, locks.size());
  }

  // Test that 2 exclusive db locks coalesce to one
  @Test
  public void testExExDb() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
  }

  // Test that existing exclusive db with new shared_write coalesces to
  // exclusive
  @Test
  public void testExSWDb() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp).setUser("fred");
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp);
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.EXCLUSIVE, locks.get(0).getType());
  }

  // Test that existing exclusive db with new shared_read coalesces to
  // exclusive
  @Test
  public void testExSRDb() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp).setUser("fred");
    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp);
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.EXCLUSIVE, locks.get(0).getType());
  }

  // Test that existing shared_write db with new exclusive coalesces to
  // exclusive
  @Test
  public void testSWExDb() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.EXCLUSIVE, locks.get(0).getType());
  }

  // Test that existing shared_write db with new shared_write coalesces to
  // shared_write
  @Test
  public void testSWSWDb() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.SHARED_WRITE, locks.get(0).getType());
  }

  // Test that existing shared_write db with new shared_read coalesces to
  // shared_write
  @Test
  public void testSWSRDb() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.SHARED_WRITE, locks.get(0).getType());
  }

  // Test that existing shared_read db with new exclusive coalesces to
  // exclusive
  @Test
  public void testSRExDb() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.EXCLUSIVE, locks.get(0).getType());
  }

  // Test that existing shared_read db with new shared_write coalesces to
  // shared_write
  @Test
  public void testSRSWDb() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.SHARED_WRITE, locks.get(0).getType());
  }

  // Test that existing shared_read db with new shared_read coalesces to
  // shared_read
  @Test
  public void testSRSRDb() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.SHARED_READ, locks.get(0).getType());
  }

  // Test that 2 separate tables don't coalesce.
  @Test
  public void testTwoSeparateTables() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("yourtable");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(2, locks.size());
  }

  // Test that 2 exclusive table locks coalesce to one
  @Test
  public void testExExTable() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
  }

  // Test that existing exclusive table with new shared_write coalesces to
  // exclusive
  @Test
  public void testExSWTable() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.EXCLUSIVE, locks.get(0).getType());
  }

  // Test that existing exclusive table with new shared_read coalesces to
  // exclusive
  @Test
  public void testExSRTable() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.EXCLUSIVE, locks.get(0).getType());
  }

  // Test that existing shared_write table with new exclusive coalesces to
  // exclusive
  @Test
  public void testSWExTable() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.EXCLUSIVE, locks.get(0).getType());
  }

  // Test that existing shared_write table with new shared_write coalesces to
  // shared_write
  @Test
  public void testSWSWTable() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.SHARED_WRITE, locks.get(0).getType());
  }

  // Test that existing shared_write table with new shared_read coalesces to
  // shared_write
  @Test
  public void testSWSRTable() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.SHARED_WRITE, locks.get(0).getType());
  }

  // Test that existing shared_read table with new exclusive coalesces to
  // exclusive
  @Test
  public void testSRExTable() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.EXCLUSIVE, locks.get(0).getType());
  }

  // Test that existing shared_read table with new shared_write coalesces to
  // shared_write
  @Test
  public void testSRSWTable() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.SHARED_WRITE, locks.get(0).getType());
  }

  // Test that existing shared_read table with new shared_read coalesces to
  // shared_read
  @Test
  public void testSRSRTable() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.SHARED_READ, locks.get(0).getType());
  }

  // Test that 2 separate partitions don't coalesce.
  @Test
  public void testTwoSeparatePartitions() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("yourpart");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(2, locks.size());
  }

  // Test that 2 exclusive partition locks coalesce to one
  @Test
  public void testExExPart() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
  }

  // Test that existing exclusive partition with new shared_write coalesces to
  // exclusive
  @Test
  public void testExSWPart() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.EXCLUSIVE, locks.get(0).getType());
  }

  // Test that existing exclusive partition with new shared_read coalesces to
  // exclusive
  @Test
  public void testExSRPart() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.EXCLUSIVE, locks.get(0).getType());
  }

  // Test that existing shared_write partition with new exclusive coalesces to
  // exclusive
  @Test
  public void testSWExPart() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.EXCLUSIVE, locks.get(0).getType());
  }

  // Test that existing shared_write partition with new shared_write coalesces to
  // shared_write
  @Test
  public void testSWSWPart() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.SHARED_WRITE, locks.get(0).getType());
  }

  // Test that existing shared_write partition with new shared_read coalesces to
  // shared_write
  @Test
  public void testSWSRPart() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.SHARED_WRITE, locks.get(0).getType());
  }

  // Test that existing shared_read partition with new exclusive coalesces to
  // exclusive
  @Test
  public void testSRExPart() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.EXCLUSIVE, locks.get(0).getType());
  }

  // Test that existing shared_read partition with new shared_write coalesces to
  // shared_write
  @Test
  public void testSRSWPart() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.SHARED_WRITE, locks.get(0).getType());
  }

  // Test that existing shared_read partition with new shared_read coalesces to
  // shared_read
  @Test
  public void testSRSRPart() {
    LockRequestBuilder bldr = new LockRequestBuilder();
    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp);
    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypart");
    bldr.addLockComponent(comp).setUser("fred");
    LockRequest req = bldr.build();
    List<LockComponent> locks = req.getComponent();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(LockType.SHARED_READ, locks.get(0).getType());
  }
}

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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.hbase.snapshot;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hcatalog.hbase.SkeletonHBaseTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRevisionManagerEndpoint extends SkeletonHBaseTest {

  @BeforeClass
  public static void setup() throws Throwable {
    // test case specific mini cluster settings
    testConf = new Configuration(false);
    testConf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      "org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpoint",
      "org.apache.hadoop.hbase.coprocessor.GenericEndpoint");
    testConf.set(RMConstants.REVISION_MGR_ENDPOINT_IMPL_CLASS, MockRM.class.getName());
    setupSkeletonHBaseTest();
  }

  /**
   * Mock implementation to test the protocol/serialization
   */
  public static class MockRM implements RevisionManager {

    private static class Invocation {
      Invocation(String methodName, Object ret, Object... args) {
        this.methodName = methodName;
        this.args = args;
        this.ret = ret;
      }

      String methodName;
      Object[] args;
      Object ret;

      private static boolean equals(Object obj1, Object obj2) {
        if (obj1 == obj2) return true;
        if (obj1 == null || obj2 == null) return false;
        if (obj1 instanceof Transaction || obj1 instanceof TableSnapshot) {
          return obj1.toString().equals(obj2.toString());
        }
        return obj1.equals(obj2);
      }

      @Override
      public boolean equals(Object obj) {
        Invocation other = (Invocation) obj;
        if (this == other) return true;
        if (other == null) return false;
        if (this.args != other.args) {
          if (this.args == null || other.args == null) return false;
          if (this.args.length != other.args.length) return false;
          for (int i = 0; i < args.length; i++) {
            if (!equals(this.args[i], other.args[i])) return false;
          }
        }
        return equals(this.ret, other.ret);
      }

      @Override
      public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("method", this.methodName).
          append("args", this.args).
          append("returns", this.ret).
          toString();
      }
    }

    final static String DEFAULT_INSTANCE = "default";
    final static Map<String, MockRM> INSTANCES = new ConcurrentHashMap<String, MockRM>();
    Invocation lastCall;
    boolean isOpen = false;

    private <T extends Object> T recordCall(T result, Object... args) {
      StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
      lastCall = new Invocation(stackTrace[2].getMethodName(), result, args);
      return result;
    }

    @Override
    public void initialize(Configuration conf) {
      if (!INSTANCES.containsKey(DEFAULT_INSTANCE))
        INSTANCES.put(DEFAULT_INSTANCE, this);
    }

    @Override
    public void open() throws IOException {
      isOpen = true;
    }

    @Override
    public void close() throws IOException {
      isOpen = false;
    }

    @Override
    public void createTable(String table, List<String> columnFamilies) throws IOException {
    }

    @Override
    public void dropTable(String table) throws IOException {
    }

    @Override
    public Transaction beginWriteTransaction(String table,
                         List<String> families) throws IOException {
      return recordCall(null, table, families);
    }

    @Override
    public Transaction beginWriteTransaction(String table,
                         List<String> families, long keepAlive) throws IOException {
      return recordCall(null, table, families, keepAlive);
    }

    @Override
    public void commitWriteTransaction(Transaction transaction)
      throws IOException {
    }

    @Override
    public void abortWriteTransaction(Transaction transaction)
      throws IOException {
    }

    @Override
    public List<FamilyRevision> getAbortedWriteTransactions(String table,
                                String columnFamily) throws IOException {
      return null;
    }

    @Override
    public TableSnapshot createSnapshot(String tableName)
      throws IOException {
      return null;
    }

    @Override
    public TableSnapshot createSnapshot(String tableName, long revision)
      throws IOException {
      TableSnapshot ret = new TableSnapshot(tableName, new HashMap<String, Long>(), revision);
      return recordCall(ret, tableName, revision);
    }

    @Override
    public void keepAlive(Transaction transaction) throws IOException {
      recordCall(null, transaction);
    }
  }

  @Test
  public void testRevisionManagerProtocol() throws Throwable {

    Configuration conf = getHbaseConf();
    RevisionManager rm = RevisionManagerFactory.getOpenedRevisionManager(
      RevisionManagerEndpointClient.class.getName(), conf);

    MockRM mockImpl = MockRM.INSTANCES.get(MockRM.DEFAULT_INSTANCE);
    Assert.assertNotNull(mockImpl);
    Assert.assertTrue(mockImpl.isOpen);

    Transaction t = new Transaction("t1", Arrays.asList("f1", "f2"), 0, 0);
    MockRM.Invocation call = new MockRM.Invocation("keepAlive", null, t);
    rm.keepAlive(t);
    Assert.assertEquals(call.methodName, call, mockImpl.lastCall);

    t = new Transaction("t2", Arrays.asList("f21", "f22"), 0, 0);
    call = new MockRM.Invocation("beginWriteTransaction", null, t.getTableName(), t.getColumnFamilies());
    call.ret = rm.beginWriteTransaction(t.getTableName(), t.getColumnFamilies());
    Assert.assertEquals(call.methodName, call, mockImpl.lastCall);

    call = new MockRM.Invocation("createSnapshot", null, "t3", 1L);
    call.ret = rm.createSnapshot("t3", 1);
    Assert.assertEquals(call.methodName, call, mockImpl.lastCall);

  }

}

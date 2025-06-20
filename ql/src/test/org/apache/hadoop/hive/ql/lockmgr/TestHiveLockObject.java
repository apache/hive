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
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Enclosed.class)
public class TestHiveLockObject {

  public static class TestHiveLockObjectNonParametrized {
    private final HiveConf conf = new HiveConf();

    @Test
    public void testEqualsAndHashCode() {
      HiveLockObjectData data1 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
          "select * from mytable", conf);
      HiveLockObjectData data2 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
          "select * from mytable", conf);
      assertEquals(data1, data2);
      assertEquals(data1.hashCode(), data2.hashCode());

      HiveLockObject obj1 = new HiveLockObject("mytable", data1);
      HiveLockObject obj2 = new HiveLockObject("mytable", data2);
      assertEquals(obj1, obj2);
      assertEquals(obj1.hashCode(), obj2.hashCode());
    }

    @Test
    public void testTruncate() {
      conf.setIntVar(HiveConf.ConfVars.HIVE_LOCK_QUERY_STRING_MAX_LENGTH, 1000000);
      HiveLockObjectData data0 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
          "01234567890", conf);
      assertEquals("With default settings query string should not be truncated",
          11, data0.getQueryStr().length());
      conf.setIntVar(HiveConf.ConfVars.HIVE_LOCK_QUERY_STRING_MAX_LENGTH, 10);
      HiveLockObjectData data1 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
          "01234567890", conf);
      HiveLockObjectData data2 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
          "0123456789", conf);
      HiveLockObjectData data3 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
          "012345678", conf);
      HiveLockObjectData data4 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
          null, conf);
      assertEquals("Long string truncation failed", 10, data1.getQueryStr().length());
      assertEquals("String truncation failed", 10, data2.getQueryStr().length());
      assertEquals("Short string should not be truncated", 9, data3.getQueryStr().length());
      assertNull("Null query string handling failed", data4.getQueryStr());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestHiveLockObjectParametrized {

    private final String data;
    private final String expectedQueryId;
    private final String expectedLockTime;
    private final String expectedLockMode;
    private final String expectedQueryStr;
    private final String expectedClientIp;

    public TestHiveLockObjectParametrized(String data, String expectedQueryId, String expectedLockTime,
                              String expectedLockMode, String expectedQueryStr, String expectedClientIp) {
      this.data = data;
      this.expectedQueryId = expectedQueryId;
      this.expectedLockTime = expectedLockTime;
      this.expectedLockMode = expectedLockMode;
      this.expectedQueryStr = expectedQueryStr;
      this.expectedClientIp = expectedClientIp;
    }

    // Method to supply the parameters for the parameterized test
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] {
          { "query1:2025-03-31:EXPLICIT:SELECT * FROM table", "query1", "2025-03-31", "EXPLICIT", "SELECT * FROM table", null },
          { "query2:2025-03-31:EXPLICIT:SELECT * FROM table:192.168.0.1", "query2", "2025-03-31", "EXPLICIT", "SELECT * FROM table", "192.168.0.1" },
          { "query3:2025-03-31:EXPLICIT:SELECT * FROM table:2001:0db8:85a3:0000:0000:8a2e:0370:7334", "query3", "2025-03-31", "EXPLICIT", "SELECT * FROM table", "2001:0db8:85a3:0000:0000:8a2e:0370:7334" },
          { "query5:2025-03-31:EXPLICIT:SELECT * FROM table:some.company.com", "query5", "2025-03-31", "EXPLICIT", "SELECT * FROM table", "some.company.com" }
      });
    }
    
    @Test
    public void testConstructor() {
      HiveLockObjectData lockObjectData = new HiveLockObjectData(data);

      assertEquals("queryId should match", expectedQueryId, lockObjectData.getQueryId());
      assertEquals("lockTime should match", expectedLockTime, lockObjectData.getLockTime());
      assertEquals("lockMode should match", expectedLockMode, lockObjectData.getLockMode());
      assertEquals("queryStr should match", expectedQueryStr, lockObjectData.getQueryStr());
      assertEquals("clientIp should match", expectedClientIp, lockObjectData.getClientIp());
    }
  }
}

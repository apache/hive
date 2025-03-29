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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestHiveLockObject {

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

  @Test
  public void testConstructor_withoutClientIp() {
    String data = "query1:2025-03-31:EXPLICIT:SELECT * FROM table";
    HiveLockObjectData lockObjectData = new HiveLockObjectData(data);

    assertEquals("queryId should match", "query1", lockObjectData.getQueryId());
    assertEquals("lockTime should match", "2025-03-31", lockObjectData.getLockTime());
    assertEquals("lockMode should match", "EXPLICIT", lockObjectData.getLockMode());
    assertEquals("queryStr should match", "SELECT * FROM table", lockObjectData.getQueryStr());
    assertNull("clientIp should be null", lockObjectData.getClientIp());
  }

  @Test
  public void testConstructor_withIPv4ClientIp() {
    String data = "query2:2025-03-31:EXPLICIT:SELECT * FROM table:192.168.0.1";
    HiveLockObjectData lockObjectData = new HiveLockObjectData(data);

    assertEquals("queryId should match", "query2", lockObjectData.getQueryId());
    assertEquals("lockTime should match", "2025-03-31", lockObjectData.getLockTime());
    assertEquals("lockMode should match", "EXPLICIT", lockObjectData.getLockMode());
    assertEquals("queryStr should match", "SELECT * FROM table", lockObjectData.getQueryStr());
    assertEquals("clientIp should match", "192.168.0.1", lockObjectData.getClientIp());
  }

  @Test
  public void testConstructor_withIPv6ClientIp() {
    String data = "query3:2025-03-31:EXPLICIT:SELECT * FROM table:2001:0db8:85a3:0000:0000:8a2e:0370:7334";
    HiveLockObjectData lockObjectData = new HiveLockObjectData(data);

    assertEquals("queryId should match", "query3", lockObjectData.getQueryId());
    assertEquals("lockTime should match", "2025-03-31", lockObjectData.getLockTime());
    assertEquals("lockMode should match", "EXPLICIT", lockObjectData.getLockMode());
    assertEquals("queryStr should match", "SELECT * FROM table", lockObjectData.getQueryStr());
    assertEquals("clientIp should match", "2001:0db8:85a3:0000:0000:8a2e:0370:7334", lockObjectData.getClientIp());
  }

  @Test
  public void testConstructor_withHostname() {
    String data = "query5:2025-03-31:EXPLICIT:SELECT * FROM table:some.company.com";
    HiveLockObjectData lockObjectData = new HiveLockObjectData(data);

    assertEquals("queryId should match", "query5", lockObjectData.getQueryId());
    assertEquals("lockTime should match", "2025-03-31", lockObjectData.getLockTime());
    assertEquals("lockMode should match", "EXPLICIT", lockObjectData.getLockMode());
    assertEquals("queryStr should match", "SELECT * FROM table", lockObjectData.getQueryStr());
    assertEquals("clientIp should match", "some.company.com", lockObjectData.getClientIp());
  }
}

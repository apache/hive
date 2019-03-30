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

import org.junit.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.junit.Test;

public class TestHiveLockObject {

  private HiveConf conf = new HiveConf();

  @Test
  public void testEqualsAndHashCode() {
    HiveLockObjectData data1 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
        "select * from mytable", conf);
    HiveLockObjectData data2 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
        "select * from mytable", conf);
    Assert.assertEquals(data1, data2);
    Assert.assertEquals(data1.hashCode(), data2.hashCode());

    HiveLockObject obj1 = new HiveLockObject("mytable", data1);
    HiveLockObject obj2 = new HiveLockObject("mytable", data2);
    Assert.assertEquals(obj1, obj2);
    Assert.assertEquals(obj1.hashCode(), obj2.hashCode());
  }

  @Test
  public void testTruncate() {
    conf.setIntVar(HiveConf.ConfVars.HIVE_LOCK_QUERY_STRING_MAX_LENGTH, 1000000);
    HiveLockObjectData data0 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
        "01234567890", conf);
    Assert.assertEquals("With default settings query string should not be truncated",
        data0.getQueryStr().length(), 11);
    conf.setIntVar(HiveConf.ConfVars.HIVE_LOCK_QUERY_STRING_MAX_LENGTH, 10);
    HiveLockObjectData data1 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
        "01234567890", conf);
    HiveLockObjectData data2 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
        "0123456789", conf);
    HiveLockObjectData data3 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
        "012345678", conf);
    HiveLockObjectData data4 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01",
        null, conf);
    Assert.assertEquals("Long string truncation failed", data1.getQueryStr().length(), 10);
    Assert.assertEquals("String truncation failed", data2.getQueryStr().length(), 10);
    Assert.assertEquals("Short string should not be truncated", data3.getQueryStr().length(), 9);
    Assert.assertNull("Null query string handling failed", data4.getQueryStr());
  }
}

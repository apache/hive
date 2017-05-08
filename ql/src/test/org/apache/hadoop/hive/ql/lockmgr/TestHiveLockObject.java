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

public class TestHiveLockObject {

  @Test
  public void testEqualsAndHashCode() {
    HiveLockObjectData data1 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01", "select * from mytable");
    HiveLockObjectData data2 = new HiveLockObjectData("ID1", "SHARED", "1997-07-01", "select * from mytable");
    Assert.assertEquals(data1, data2);
    Assert.assertEquals(data1.hashCode(), data2.hashCode());

    HiveLockObject obj1 = new HiveLockObject("mytable", data1);
    HiveLockObject obj2 = new HiveLockObject("mytable", data2);
    Assert.assertEquals(obj1, obj2);
    Assert.assertEquals(obj1.hashCode(), obj2.hashCode());
  }

}

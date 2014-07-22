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
package org.apache.hadoop.hive.ql.exec.persistence;

import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestMapJoinRowContainer {
  
  @Test
  public void testSerialization() throws Exception {
    MapJoinRowContainer container1 = new MapJoinEagerRowContainer();
    container1.addRow(new Object[]{ new Text("f0"), null, new ShortWritable((short)0xf)});
    container1.addRow(Arrays.asList(new Object[]{ null, new Text("f1"), new ShortWritable((short)0xf)}));
    container1.addRow(new Object[]{ null, null, new ShortWritable((short)0xf)});
    container1.addRow(Arrays.asList(new Object[]{ new Text("f0"), new Text("f1"), new ShortWritable((short)0x1)}));
    MapJoinRowContainer container2 = Utilities.serde(container1, "f0,f1,filter", "string,string,smallint");
    Utilities.testEquality(container1, container2);
    Assert.assertEquals(4, container1.rowCount());
    Assert.assertEquals(1, container2.getAliasFilter());
  }

}

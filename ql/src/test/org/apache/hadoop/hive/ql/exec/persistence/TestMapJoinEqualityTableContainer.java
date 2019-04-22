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
package org.apache.hadoop.hive.ql.exec.persistence;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class TestMapJoinEqualityTableContainer {
  
  private static final MapJoinKeyObject KEY1 = new MapJoinKeyObject(new Object[] {new Text("key1")});
  private static final MapJoinKeyObject KEY2 = new MapJoinKeyObject(new Object[] {new Text("key2")});
  private static final MapJoinKeyObject KEY3 = new MapJoinKeyObject(new Object[] {new Text("key3")});
  private static final MapJoinKeyObject KEY4 = new MapJoinKeyObject(new Object[] {new Text("key4")});
  private static final Object[] VALUE = new Object[] {new Text("value")};
  private HashMapWrapper container;
  private MapJoinRowContainer rowContainer;
  @Before
  public void setup() throws Exception {
    rowContainer = new MapJoinEagerRowContainer();
    rowContainer.addRow(VALUE);
    container = new HashMapWrapper();
  }
  @Test
  public void testContainerBasics() throws Exception {
    container.put(KEY1, rowContainer);
    container.put(KEY2, rowContainer);
    container.put(KEY3, rowContainer);
    container.put(KEY4, rowContainer);
    Assert.assertEquals(4, container.size());
    Map<MapJoinKey, MapJoinRowContainer> localContainer =
        new HashMap<MapJoinKey, MapJoinRowContainer>();
    for(Entry<MapJoinKey, MapJoinRowContainer> entry : container.entrySet()) {
      localContainer.put(entry.getKey(), entry.getValue());
    }
    Utilities.testEquality(container.get(KEY1), localContainer.get(KEY1));
    Utilities.testEquality(container.get(KEY2), localContainer.get(KEY2));
    Utilities.testEquality(container.get(KEY3), localContainer.get(KEY3));
    Utilities.testEquality(container.get(KEY4), localContainer.get(KEY4));
    container.clear();
    Assert.assertEquals(0, container.size());
    Assert.assertTrue(container.entrySet().isEmpty());
  }
}
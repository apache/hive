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

import junit.framework.Assert;

import org.junit.Test;

public class TestMapJoinKeys {

  @Test
  public void testDoubleKeys() {
    MapJoinDoubleKeys left = new MapJoinDoubleKeys(148, null);
    MapJoinDoubleKeys right = new MapJoinDoubleKeys(148, null);
    Assert.assertEquals(left, right);
    Assert.assertEquals(left.hashCode(), right.hashCode());
    left = new MapJoinDoubleKeys(null, 148);
    right = new MapJoinDoubleKeys(null, 148);
    Assert.assertEquals(left, right);
    Assert.assertEquals(left.hashCode(), right.hashCode());
    left = new MapJoinDoubleKeys(148, null);
    right = new MapJoinDoubleKeys(149, null);
    Assert.assertFalse(left.equals(right));
    Assert.assertFalse(right.equals(left));

  }
  @Test
  public void testObjectKeys() {
    MapJoinObjectKey left = new MapJoinObjectKey(new Object[]{null, "left"});
    MapJoinObjectKey right = new MapJoinObjectKey(new Object[]{null, "right"});
    Assert.assertFalse(left.equals(right));
    Assert.assertFalse(right.equals(left));
    left = new MapJoinObjectKey(new Object[]{"key", 148, null});
    right = new MapJoinObjectKey(new Object[]{"key", 148, null});
    Assert.assertEquals(left, right);
    Assert.assertEquals(left.hashCode(), right.hashCode());
  }
}

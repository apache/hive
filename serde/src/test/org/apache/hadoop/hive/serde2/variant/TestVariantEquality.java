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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.serde2.variant;

import org.junit.Test;

import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variant;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestVariantEquality {

  @Test
  public void testEqualBytesAreEqual() throws Exception {
    Variant a = variant("{\"a\":1,\"b\":[true,null]}");
    Variant b = new Variant(a.getValue().clone(), a.getMetadata().clone());
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testDifferentValueIsNotEqual() throws Exception {
    Variant one = variant("1");
    Variant two = variant("2");
    assertEquals(one.getMetadata().length, two.getMetadata().length);
    assertNotEquals(one, two);
  }

  @Test
  public void testDifferentMetadataIsNotEqual() throws Exception {
    Variant a = variant("{\"a\":1}");
    Variant b = new Variant(a.getValue(), variant("{\"b\":1}").getMetadata());
    assertNotEquals(a, b);
  }

  @Test
  public void testSubVariantEqualsTheStandaloneValue() throws Exception {
    Variant element = variant("[1,2]").getElementAtIndex(1);
    Variant standalone = variant("2");
    assertEquals(standalone, element);
    assertEquals(element, standalone);
    assertEquals(standalone.hashCode(), element.hashCode());
  }

  @Test
  public void testNotEqualToOtherObjects() throws Exception {
    Variant a = variant("1");
    assertNotEquals(a, null);
    assertNotEquals(a, "1");
  }
}

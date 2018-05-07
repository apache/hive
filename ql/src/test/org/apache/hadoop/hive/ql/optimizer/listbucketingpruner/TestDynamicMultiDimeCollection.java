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
package org.apache.hadoop.hive.ql.optimizer.listbucketingpruner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Test {@link DynamicMultiDimeContainer}
 *
 */
public class TestDynamicMultiDimeCollection extends TestCase {
  private static String DEF_DIR = "default";

  @Test
  public void testUniqueElementsList1() {
    // (1,a,x), (2,b,x), (1,c,x), (2,a,y)
    List<List<String>> values = new ArrayList<List<String>>();
    List<String> v1 = Arrays.asList("1", "a", "x");
    List<String> v2 = Arrays.asList("2", "b", "x");
    List<String> v3 = Arrays.asList("1", "c", "x");
    List<String> v4 = Arrays.asList("2", "a", "y");
    values.add(v1);
    values.add(v2);
    values.add(v3);
    values.add(v4);
    List<List<String>> actuals = ListBucketingPruner.DynamicMultiDimensionalCollection
        .uniqueElementsList(values, DEF_DIR);

    List<List<String>> expecteds = new ArrayList<List<String>>();
    v1 = Arrays.asList("1", "2", "default");
    v2 = Arrays.asList("a", "b", "c", "default");
    v3 = Arrays.asList("x", "y", "default");
    expecteds.add(v1);
    expecteds.add(v2);
    expecteds.add(v3);

    Assert.assertEquals(expecteds, actuals);
  }

  @Test
  public void testUniqueElementsList2() {
    // (1,a,x), (2,b,x), (1,c,x), (2,a,y)
    List<List<String>> values = new ArrayList<List<String>>();
    List<String> v1 = Arrays.asList("1", "a", "x");
    values.add(v1);
    List<List<String>> actuals = ListBucketingPruner.DynamicMultiDimensionalCollection
        .uniqueElementsList(values, DEF_DIR);
    List<List<String>> expecteds = new ArrayList<List<String>>();
    v1 = Arrays.asList("1", "default");
    List<String> v2 = Arrays.asList("a", "default");
    List<String> v3 = Arrays.asList("x", "default");
    expecteds.add(v1);
    expecteds.add(v2);
    expecteds.add(v3);

    Assert.assertEquals(expecteds, actuals);
  }

  @Test
  public void testUniqueElementsList3() {
    // (1,a,x), (2,b,x), (1,c,x), (2,a,y)
    List<List<String>> values = new ArrayList<List<String>>();
    List<String> v1 = Arrays.asList("1");
    List<String> v2 = Arrays.asList("2");
    List<String> v3 = Arrays.asList("3");
    List<String> v4 = Arrays.asList("4");
    values.add(v1);
    values.add(v2);
    values.add(v3);
    values.add(v4);
    List<List<String>> actuals = ListBucketingPruner.DynamicMultiDimensionalCollection
        .uniqueElementsList(values, DEF_DIR);
    List<List<String>> expecteds = new ArrayList<List<String>>();
    v1 = Arrays.asList("1", "2", "3", "4", "default");
    expecteds.add(v1);

    Assert.assertEquals(expecteds, actuals);
  }

  @Test
  public void testFlat3() throws SemanticException {
    List<List<String>> uniqSkewedElements = new ArrayList<List<String>>();
    List<String> v1 = Arrays.asList("1", "2", "default");
    List<String> v2 = Arrays.asList("a", "b", "c", "default");
    List<String> v3 = Arrays.asList("x", "y", "default");
    uniqSkewedElements.add(v1);
    uniqSkewedElements.add(v2);
    uniqSkewedElements.add(v3);
    List<List<String>> actuals = ListBucketingPruner.DynamicMultiDimensionalCollection
        .flat(uniqSkewedElements);
    Assert.assertTrue(actuals.size() == 36);
  }

  @Test
  public void testFlat2() throws SemanticException {
    List<List<String>> uniqSkewedElements = new ArrayList<List<String>>();
    List<String> v1 = Arrays.asList("1", "2");
    uniqSkewedElements.add(v1);
    List<List<String>> actual = ListBucketingPruner.DynamicMultiDimensionalCollection
        .flat(uniqSkewedElements);
    List<List<String>> expected = new ArrayList<List<String>>();
    v1 = Arrays.asList("1");
    List<String> v2 = Arrays.asList("2");
    expected.add(v1);
    expected.add(v2);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFlat1() throws SemanticException {
    List<List<String>> uniqSkewedElements = new ArrayList<List<String>>();
    List<String> v1 = Arrays.asList("1", "2");
    List<String> v2 = Arrays.asList("3", "4");
    uniqSkewedElements.add(v1);
    uniqSkewedElements.add(v2);
    List<List<String>> actual = ListBucketingPruner.DynamicMultiDimensionalCollection
        .flat(uniqSkewedElements);
    List<List<String>> expected = new ArrayList<List<String>>();
    v1 = Arrays.asList("1", "3");
    v2 = Arrays.asList("1", "4");
    List<String> v3 = Arrays.asList("2", "3");
    List<String> v4 = Arrays.asList("2", "4");
    expected.add(v1);
    expected.add(v2);
    expected.add(v3);
    expected.add(v4);
    Assert.assertEquals(expected, actual);
  }

}

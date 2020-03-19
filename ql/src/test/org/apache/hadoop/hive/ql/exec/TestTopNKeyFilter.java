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
package org.apache.hadoop.hive.ql.exec;

import static org.apache.hadoop.hive.ql.exec.vector.VectorTopNKeyOperator.checkTopNFilterEfficiency;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test of TopNKeyFilter.
 */
public class TestTopNKeyFilter {
  private static final Logger LOG = LoggerFactory.getLogger(TestTopNKeyFilter.class.getName());
  public static final Comparator<TestKeyWrapper> TEST_KEY_WRAPPER_COMPARATOR = Comparator.comparingInt(o -> o.keyValue);

  @Test
  public void testNothingCanBeForwardedIfTopNIs0() {
    TopNKeyFilter topNKeyFilter = new TopNKeyFilter(0, TEST_KEY_WRAPPER_COMPARATOR);
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(1)), is(false));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(-1)), is(false));

    assertEquals(0, topNKeyFilter.getEffectiveBoundaryChecks());
    assertEquals(0, topNKeyFilter.getRepeated());
    assertEquals(0, topNKeyFilter.getKeySetSize());
  }

  @Test
  public void testFirstTopNKeysCanBeForwarded() {
    TopNKeyFilter topNKeyFilter = new TopNKeyFilter(3, TEST_KEY_WRAPPER_COMPARATOR);
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(1)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(5)), is(true));

    assertEquals(0, topNKeyFilter.getEffectiveBoundaryChecks());
    assertEquals(0, topNKeyFilter.getRepeated());

    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(10)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(11)), is(false));
    assertEquals(1, topNKeyFilter.getEffectiveBoundaryChecks());

    assertEquals(3, topNKeyFilter.getKeySetSize());

    //new key same as boundary element (in 1,5,10)
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(10)), is(true));
    assertEquals(2, topNKeyFilter.getEffectiveBoundaryChecks());
  }

  @Test
  public void testFirstTopNKeysCanBeForwardedDesc() {
    TopNKeyFilter topNKeyFilter = new TopNKeyFilter(3, TEST_KEY_WRAPPER_COMPARATOR.reversed());
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(10)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(5)), is(true));

    assertEquals(0, topNKeyFilter.getEffectiveBoundaryChecks());
    assertEquals(0, topNKeyFilter.getRepeated());

    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(1)), is(true));

    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(11)), is(true));
    assertEquals(0, topNKeyFilter.getEffectiveBoundaryChecks());
    assertEquals(0, topNKeyFilter.getRepeated());

    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(0)), is(false));
    assertEquals(1, topNKeyFilter.getEffectiveBoundaryChecks());

    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(11)), is(true));
    assertEquals(1, topNKeyFilter.getRepeated());

    assertEquals(3, topNKeyFilter.getKeySetSize());

    //new key same as boundary element (in 11, 10, 5)
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(5)), is(true));
    assertEquals(2, topNKeyFilter.getEffectiveBoundaryChecks());
  }

  @Test
  public void testKeyCanNotBeForwardedIfItIsDroppedOutFromTopNKeys() {
    TopNKeyFilter topNKeyFilter = new TopNKeyFilter(2, TEST_KEY_WRAPPER_COMPARATOR);
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(1)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(3)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(2)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(3)), is(false));
  }

  @Test
  public void testMembersOfTopNKeysStillCanBeForwardedAfterNonTopNKeysTried() {
    TopNKeyFilter topNKeyFilter = new TopNKeyFilter(2, TEST_KEY_WRAPPER_COMPARATOR);
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(1)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(3)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(5)), is(false));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(3)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(1)), is(true));
  }

  @Test
  public void testEfficiencyWhenEverythingIsForwarded() {
    TopNKeyFilter topNKeyFilter = new TopNKeyFilter(2, TEST_KEY_WRAPPER_COMPARATOR);
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(5)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(4)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(3)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(2)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(1)), is(true));
    assertThat(topNKeyFilter.forwardingRatio(), is(1.0f));
  }

  @Test
  public void testEfficiencyWhenOnlyOneIsForwarded() {
    TopNKeyFilter topNKeyFilter = new TopNKeyFilter(1, TEST_KEY_WRAPPER_COMPARATOR);
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(1)), is(true));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(2)), is(false));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(3)), is(false));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(4)), is(false));
    assertThat(topNKeyFilter.canForward(new TestKeyWrapper(5)), is(false));
    assertThat(topNKeyFilter.forwardingRatio(), is(1/5f));
  }

  @Test
  public void testDisabling() {
    TopNKeyFilter efficientFilter = new TopNKeyFilter(1, TEST_KEY_WRAPPER_COMPARATOR);
    efficientFilter.canForward(new TestKeyWrapper(1));
    efficientFilter.canForward(new TestKeyWrapper(2));
    efficientFilter.canForward(new TestKeyWrapper(3));

    TopNKeyFilter inefficientFilter = new TopNKeyFilter(1, TEST_KEY_WRAPPER_COMPARATOR);
    inefficientFilter.canForward(new TestKeyWrapper(3));
    inefficientFilter.canForward(new TestKeyWrapper(2));
    inefficientFilter.canForward(new TestKeyWrapper(1));

    Map<KeyWrapper, TopNKeyFilter> filters = new HashMap<KeyWrapper, TopNKeyFilter>() {{
      put(new TestKeyWrapper(100), efficientFilter);
      put(new TestKeyWrapper(200), inefficientFilter);
    }};

    Set<KeyWrapper> disabled = new HashSet<>();
    checkTopNFilterEfficiency(filters, disabled, 0.6f, LOG, 1);
    assertThat(disabled, hasSize(1));
    assertThat(disabled, hasItem(new TestKeyWrapper(200)));
  }

  /**
   * Test implementation of KeyWrapper.
   */
  private static class TestKeyWrapper extends KeyWrapper {

    private final int keyValue;

    TestKeyWrapper(int keyValue) {
      this.keyValue = keyValue;
    }

    @Override
    public void getNewKey(Object row, ObjectInspector rowInspector) throws HiveException {

    }

    @Override
    public void setHashKey() {

    }

    @Override
    public KeyWrapper copyKey() {
      return new TestKeyWrapper(this.keyValue);
    }

    @Override
    public void copyKey(KeyWrapper oldWrapper) {

    }

    @Override
    public Object[] getKeyArray() {
      return new Object[0];
    }

    @Override
    public boolean isCopy() {
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestKeyWrapper that = (TestKeyWrapper) o;
      return keyValue == that.keyValue;
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyValue);
    }
  }
}

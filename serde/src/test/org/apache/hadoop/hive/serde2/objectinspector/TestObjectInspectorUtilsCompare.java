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
package org.apache.hadoop.hive.serde2.objectinspector;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.NullValueOption;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.NullValueOption.MAXVALUE;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.NullValueOption.MINVALUE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

/**
 * TestObjectInspectorUtils.compare method.
 * int compare(Object[] o1, ObjectInspector[] oi1, Object[] o2,
 *       ObjectInspector[] oi2, boolean[] columnSortOrderIsDesc, NullValueOption[] nullSortOrder)
 */
public class TestObjectInspectorUtilsCompare {

  private Integer[] objects1;
  private ObjectInspector[] oi1;
  private Integer[] objects2;
  private ObjectInspector[] oi2;
  boolean[] orderDesc;
  NullValueOption[] nullValueOption;

  void o1(Integer... objects1) {
    this.objects1 = objects1;
    this.oi1 = createOI(objects1.length);
  }

  private ObjectInspector[] createOI(int length) {
    ObjectInspector[] objectInspectors = new ObjectInspector[length];
    for (int i = 0; i < length; ++i) {
      objectInspectors[i] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }
    return objectInspectors;
  }

  void o2(Integer... objects2) {
    this.objects2 = objects2;
    this.oi2 = createOI(objects2.length);
  }

  void order(boolean... isDesc) {
    this.orderDesc = isDesc;
  }

  void nullOrder(NullValueOption... nullValueOption) {
    this.nullValueOption = nullValueOption;
  }

  @Test
  public void testWhenTheFirstValueIsSmallerMinusOneIsReturned() {
    o1(1);
    o2(2);
    order(false);
    nullOrder(MAXVALUE);

    assertThat(ObjectInspectorUtils.compare(objects1, oi1, objects2, oi2, orderDesc, nullValueOption), is(-1));
  }

  @Test
  public void testWhenTheFirstValueIsLargerOneIsReturned() {
    o1(2);
    o2(1);
    order(false);
    nullOrder(MAXVALUE);

    assertThat(ObjectInspectorUtils.compare(objects1, oi1, objects2, oi2, orderDesc, nullValueOption), is(1));
  }

  @Test
  public void testWhenTheFirstValueIsSmallerAndDescendingOrderOneIsReturned() {
    o1(1);
    o2(2);
    order(true);
    nullOrder(MAXVALUE);

    assertThat(ObjectInspectorUtils.compare(objects1, oi1, objects2, oi2, orderDesc, nullValueOption), is(1));
  }

  @Test
  public void testWhenValuesAreEqual0IsReturned() {
    o1(2, 2, 2);
    o2(2, 2, 2);
    order(false, true, false);
    nullOrder(MAXVALUE, MINVALUE, MAXVALUE);

    assertThat(ObjectInspectorUtils.compare(objects1, oi1, objects2, oi2, orderDesc, nullValueOption), is(0));
  }

  @Test
  public void testWhenTheFirstPairIsEqualsButTheSecondNot() {
    o1(2, 1, 2);
    o2(2, 2, 2);
    order(false, false, false);
    nullOrder(MAXVALUE, MAXVALUE, MAXVALUE);

    assertThat(ObjectInspectorUtils.compare(objects1, oi1, objects2, oi2, orderDesc, nullValueOption), is(-1));
  }

  @Test
  public void testWhenTheFirstValueIsNullAndNullsFirstMinusOneIsReturned() {
    o1(new Integer[]{null});
    o2(2);
    order(false);
    nullOrder(MINVALUE);

    assertThat(ObjectInspectorUtils.compare(objects1, oi1, objects2, oi2, orderDesc, nullValueOption), is(-1));
  }

  @Test
  public void testWhenTheFirstValueIsNullAndNullsLastOneIsReturned() {
    o1(new Integer[]{null});
    o2(2);
    order(false);
    nullOrder(MAXVALUE);

    assertThat(ObjectInspectorUtils.compare(objects1, oi1, objects2, oi2, orderDesc, nullValueOption), is(1));
  }

  @Test
  public void testWhenTheFirstValueIsNullAndNullsFirstAndDescendingOrderMinusOneIsReturned() {
    o1(new Integer[]{null});
    o2(2);
    order(true);
    nullOrder(MINVALUE);

    assertThat(ObjectInspectorUtils.compare(objects1, oi1, objects2, oi2, orderDesc, nullValueOption), is(-1));
  }

  @Test
  public void testWhenTheFirstValueIsNullAndNullsLastAndDescendingOrderOneIsReturned() {
    o1(new Integer[]{null});
    o2(2);
    order(true);
    nullOrder(MAXVALUE);

    assertThat(ObjectInspectorUtils.compare(objects1, oi1, objects2, oi2, orderDesc, nullValueOption), is(1));
  }

  @Test
  public void testWhenTheSecondValueIsNullAndNullsFirstOneIsReturned() {
    o1(2);
    o2(new Integer[]{null});
    order(false);
    nullOrder(MINVALUE);

    assertThat(ObjectInspectorUtils.compare(objects1, oi1, objects2, oi2, orderDesc, nullValueOption), is(1));
  }

  @Test
  public void testWhenTheSecondValueIsNullAndNullsLastMinusOneIsReturned() {
    o1(2);
    o2(new Integer[]{null});
    order(false);
    nullOrder(MAXVALUE);

    assertThat(ObjectInspectorUtils.compare(objects1, oi1, objects2, oi2, orderDesc, nullValueOption), is(-1));
  }

  @Test
  public void testWhenTheSecondValueIsNullAndNullsFirstAndDescendingOrderOneIsReturned() {
    o1(2);
    o2(new Integer[]{null});
    order(true);
    nullOrder(MINVALUE);

    assertThat(ObjectInspectorUtils.compare(objects1, oi1, objects2, oi2, orderDesc, nullValueOption), is(1));
  }

  @Test
  public void testWhenTheSecondValueIsNullAndNullsLastAndDescendingOrderMinusOneIsReturned() {
    o1(2);
    o2(new Integer[]{null});
    order(true);
    nullOrder(MAXVALUE);

    assertThat(ObjectInspectorUtils.compare(objects1, oi1, objects2, oi2, orderDesc, nullValueOption), is(-1));
  }
}

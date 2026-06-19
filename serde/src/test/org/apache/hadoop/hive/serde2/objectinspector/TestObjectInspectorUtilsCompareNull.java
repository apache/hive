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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * TestObjectInspectorUtils.compareNull method.
 */
public class TestObjectInspectorUtilsCompareNull {

  @Test
  public void testcompareNullReturns0WhenBothInputsAreNull() {
    assertThat(ObjectInspectorUtils.compareNull(null, null), is(0));
  }

  @Test
  public void testcompareNullReturns1WhenFirstInputIsNull() {
    assertThat(ObjectInspectorUtils.compareNull(null, 1), is(1));
  }

  @Test
  public void testcompareNullReturnsMinus1WhenSecondInputIsNull() {
    assertThat(ObjectInspectorUtils.compareNull(1, null), is(-1));
  }

  @Test
  public void testcompareNullReturns0WhenNoneOfTheInputsAreNull() {
    assertThat(ObjectInspectorUtils.compareNull(1, 1), is(0));
  }
}

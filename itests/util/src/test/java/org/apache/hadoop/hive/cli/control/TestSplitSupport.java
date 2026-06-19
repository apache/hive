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
package org.apache.hadoop.hive.cli.control;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestSplitSupport {

  @Test
  public void testIsSplitClass1() {
    Class<?> mainClass = org.apache.hadoop.hive.cli.control.splitsupport.SplitSupportDummy.class;
    Class<?> split0Class =
        org.apache.hadoop.hive.cli.control.splitsupport.split0.SplitSupportDummy.class;
    assertFalse(SplitSupport.isSplitClass(mainClass));
    assertTrue(SplitSupport.isSplitClass(split0Class));
  }

  @Test
  public void testGetSplitIndex0() {
    Class<?> split0Class = org.apache.hadoop.hive.cli.control.splitsupport.split0.SplitSupportDummy.class;
    assertEquals(0, SplitSupport.getSplitIndex(split0Class));
  }

  @Test
  public void testGetSplitIndex125() {
    Class<?> split0Class = org.apache.hadoop.hive.cli.control.splitsupport.split125.SplitSupportDummy.class;
    assertEquals(125, SplitSupport.getSplitIndex(split0Class));
  }
}

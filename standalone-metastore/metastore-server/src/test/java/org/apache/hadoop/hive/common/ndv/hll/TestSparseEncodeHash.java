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
package org.apache.hadoop.hive.common.ndv.hll;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
@Category(MetastoreUnitTest.class)
public class TestSparseEncodeHash {

  private long input;
  private int expected;

  public TestSparseEncodeHash(long i, int e) {
    this.input = i;
    this.expected = e;
  }

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { 11111111111L, 373692871 },
        { 4314495982023L, -1711269433 }, { 4314529536455L, -1744823865 },
        { 4314563074503L, 268425671 }, { 17257983908295L, -1644160569 }, { 536861127L, 536861127 },
        { 536844743L, 536844743 }, { 144115188075862471L, -671082041 } };
    return Arrays.asList(data);
  }

  @Test
  public void testEncodeHash() {
    HLLSparseRegister reg = new HLLSparseRegister(14, 25, 6);
    int got = reg.encodeHash(input);
    assertEquals(expected, got);
  }
}

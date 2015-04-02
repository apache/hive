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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.hbase.utils;

import org.apache.hadoop.hive.metastore.hbase.utils.BitVector;
import org.junit.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBitVector {

  static int BIT_VECTOR_SIZE = 32;
  BitVector bitVector;

  @BeforeClass
  public static void beforeTest() {
  }

  @AfterClass
  public static void afterTest() {
  }

  @Before
  public void setUp() {
    // Create a new BitVector
    bitVector = new BitVector(BIT_VECTOR_SIZE);
  }


  @After
  public void tearDown() {
  }

  @Test
  public void testSetAll() {
    // Set bits
    bitVector.setAll();
    Assert.assertEquals("11111111111111111111111111111111", bitVector.toString());
  }

  @Test
  public void testClearAll() {
    // Clear all bits
    bitVector.clearAll();
    Assert.assertEquals("00000000000000000000000000000000", bitVector.toString());
  }

  @Test
  public void testSetUnsetBit() {
    // Set 3rd bit
    bitVector.setBit(2);
    Assert.assertEquals("00100000000000000000000000000000", bitVector.toString());
    // Now check if 3rd bit is set
    Assert.assertTrue(bitVector.isBitSet(2));
    // Now set 30th bit
    bitVector.setBit(29);
    Assert.assertEquals("00100000000000000000000000000100", bitVector.toString());
    // Now check if 30th bit is set
    Assert.assertTrue(bitVector.isBitSet(29));

    // Now unset 3rd bit
    bitVector.unSetBit(2);
    Assert.assertEquals("00000000000000000000000000000100", bitVector.toString());
    // Now check if 3rd bit is unset
    Assert.assertFalse(bitVector.isBitSet(2));
    // Now unset 30th bit
    bitVector.unSetBit(29);
    Assert.assertEquals("00000000000000000000000000000000", bitVector.toString());
    // Now check if 30th bit is unset
    Assert.assertFalse(bitVector.isBitSet(29));
  }
}

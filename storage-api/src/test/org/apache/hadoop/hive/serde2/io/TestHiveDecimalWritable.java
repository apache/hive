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

package org.apache.hadoop.hive.serde2.io;

import org.junit.*;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

import org.apache.hadoop.hive.common.type.HiveDecimal;

/**
 * Unit tests for tsting the fast allocation-free conversion
 * between HiveDecimalWritable and Decimal128
 */
public class TestHiveDecimalWritable {

  @Test
  public void testHiveDecimalWritable() {

    HiveDecimalWritable decWritable;

    HiveDecimal nullDec = null;
    decWritable = new HiveDecimalWritable(nullDec);
    assertTrue(!decWritable.isSet());
    decWritable = new HiveDecimalWritable("1");
    assertTrue(decWritable.isSet());

    // UNDONE: more!
  }
}


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

package org.apache.hadoop.hive.ql.io.parquet.serde.primitive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

public class TestParquetShortInspector {

  private ParquetShortInspector inspector;

  @Before
  public void setUp() {
    inspector = new ParquetShortInspector();
  }

  @Test
  public void testShortWritable() {
    ShortWritable obj = new ShortWritable((short) 5);
    assertEquals(obj, inspector.getPrimitiveWritableObject(obj));
    assertEquals((short) 5, inspector.getPrimitiveJavaObject(obj));
  }

  @Test
  public void testIntWritable() {
    IntWritable obj = new IntWritable(10);
    assertEquals(new ShortWritable((short) 10), inspector.getPrimitiveWritableObject(obj));
    assertEquals((short) 10, inspector.getPrimitiveJavaObject(obj));
  }

  @Test
  public void testNull() {
    assertNull(inspector.getPrimitiveWritableObject(null));
    assertNull(inspector.getPrimitiveJavaObject(null));
  }

  @Test
  public void testCreate() {
    assertEquals(new ShortWritable((short) 8), inspector.create((short) 8));
  }

  @Test
  public void testSet() {
    ShortWritable obj = new ShortWritable();
    assertEquals(new ShortWritable((short) 12), inspector.set(obj, (short) 12));
  }

  @Test
  public void testGet() {
    ShortWritable obj = new ShortWritable((short) 15);
    assertEquals((short) 15, inspector.get(obj));
  }
}

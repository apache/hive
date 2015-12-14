/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.worker;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hive.hcatalog.streaming.mutate.MutableRecord;
import org.junit.Test;

public class TestRecordInspectorImpl {

  private static final int ROW_ID_COLUMN = 2;

  private RecordInspectorImpl inspector = new RecordInspectorImpl(ObjectInspectorFactory.getReflectionObjectInspector(
      MutableRecord.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA), ROW_ID_COLUMN);

  @Test
  public void testExtractRecordIdentifier() {
    RecordIdentifier recordIdentifier = new RecordIdentifier(10L, 4, 20L);
    MutableRecord record = new MutableRecord(1, "hello", recordIdentifier);
    assertThat(inspector.extractRecordIdentifier(record), is(recordIdentifier));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotAStructObjectInspector() {
    new RecordInspectorImpl(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector, 2);
  }

}

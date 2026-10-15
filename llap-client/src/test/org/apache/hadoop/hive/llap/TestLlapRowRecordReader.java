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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.llap;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructEncodedVariantObjectInspector;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestLlapRowRecordReader {

  @Test
  public void testVariantIsConvertedToJson() throws Exception {
    Variant variant = VariantBuilder.parseJson("{\"a\":1,\"b\":[true,null]}", false);

    assertEquals("{\"a\":1,\"b\":[true,null]}",
        LlapRowRecordReader.convertValue(variant, ObjectInspectorFactory.getStandardVariantObjectInspector()));
    // what the LazyBinarySerDe of the external client hands over
    assertEquals("{\"a\":1,\"b\":[true,null]}", LlapRowRecordReader.convertValue(VariantEncoding.fields(variant),
        new StructEncodedVariantObjectInspector(VariantEncoding.JAVA_OBJECT_INSPECTOR)));
  }

  @Test
  public void testNestedVariantIsConvertedToJson() throws Exception {
    Variant variant = VariantBuilder.parseJson("42", false);

    assertEquals(List.of("42"), LlapRowRecordReader.convertValue(List.of(variant),
        ObjectInspectorFactory.getStandardListObjectInspector(
            ObjectInspectorFactory.getStandardVariantObjectInspector())));
  }

  @Test
  public void testNullVariantStaysNull() {
    assertNull(LlapRowRecordReader.convertValue(null, ObjectInspectorFactory.getStandardVariantObjectInspector()));
  }
}

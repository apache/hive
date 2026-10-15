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
package org.apache.hadoop.hive.ql.io.parquet.convert;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class TestVariantGroupConverter {

  @Test
  public void testVariantHintReadsTheTwoBinariesAsAGroup() throws Exception {
    GroupType group = Types.optionalGroup()
        .optional(PrimitiveTypeName.BINARY).named("metadata")
        .optional(PrimitiveTypeName.BINARY).named("value")
        .named("v");
    Variant variant = VariantBuilder.parseJson("{\"a\":1}", false);
    MyConverterParent parent = new MyConverterParent();

    HiveGroupConverter converter = HiveGroupConverter.getConverterFromDescription(
        group, 0, parent, TypeInfoFactory.getVariantTypeInfo());
    converter.start();
    converter.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromConstantByteArray(variant.getMetadata()));
    converter.getConverter(1).asPrimitiveConverter().addBinary(Binary.fromConstantByteArray(variant.getValue()));
    converter.end();

    Writable[] fields = ((ArrayWritable) parent.getValue()).get();
    assertArrayEquals(variant.getMetadata(), ((BytesWritable) fields[0]).copyBytes());
    assertArrayEquals(variant.getValue(), ((BytesWritable) fields[1]).copyBytes());
  }
}

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

package org.apache.hadoop.hive.serde2.variant;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.VariantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * How Hive serdes store a variant: a struct of the metadata binary and the value binary, in this order. These are
 * the bytes Hive wrote for {@code struct<metadata:binary,value:binary>} before VARIANT became a category of its own.
 * Only format code uses it (serdes, vector batches, readers and writers).
 */
public final class VariantEncoding {

  public static final String METADATA = "metadata";
  public static final String VALUE = "value";
  /** Positions of the two binaries in the struct. */
  public static final int METADATA_INDEX = 0;
  public static final int VALUE_INDEX = 1;

  public static final StructTypeInfo TYPE_INFO = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(
      List.of(METADATA, VALUE), List.<TypeInfo>of(TypeInfoFactory.binaryTypeInfo, TypeInfoFactory.binaryTypeInfo));

  public static final StructObjectInspector JAVA_OBJECT_INSPECTOR =
      ObjectInspectorFactory.getStandardStructObjectInspector(List.of(METADATA, VALUE),
          List.<ObjectInspector>of(PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector,
              PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector));

  private VariantEncoding() {
  }

  /** The struct fields of {@code variant}, for {@link #JAVA_OBJECT_INSPECTOR}. */
  public static List<Object> fields(Variant variant) {
    return List.of(variant.getMetadata(), variant.getValue());
  }

  /** The struct fields of the variant that {@code oi}, a {@link VariantObjectInspector}, reads from {@code data}. */
  public static List<Object> fields(ObjectInspector oi, Object data) {
    return fields(((VariantObjectInspector) oi).getVariant(data));
  }
}

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

package org.apache.iceberg.mr.hive.serde.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergFixedObjectInspector {

  @Test
  public void testIcebergFixedObjectInspector() {
    IcebergFixedObjectInspector oi = IcebergFixedObjectInspector.get();

    Assertions.assertEquals(ObjectInspector.Category.PRIMITIVE, oi.getCategory());
    Assertions.assertEquals(PrimitiveObjectInspector.PrimitiveCategory.BINARY,
        oi.getPrimitiveCategory());

    Assertions.assertEquals(TypeInfoFactory.binaryTypeInfo, oi.getTypeInfo());
    Assertions.assertEquals(TypeInfoFactory.binaryTypeInfo.getTypeName(), oi.getTypeName());

    Assertions.assertEquals(byte[].class, oi.getJavaPrimitiveClass());
    Assertions.assertEquals(BytesWritable.class, oi.getPrimitiveWritableClass());

    Assertions.assertNull(oi.copyObject(null));
    Assertions.assertNull(oi.getPrimitiveJavaObject(null));
    Assertions.assertNull(oi.getPrimitiveWritableObject(null));
    Assertions.assertNull(oi.convert(null));

    byte[] bytes = new byte[] { 0, 1 };
    BytesWritable bytesWritable = new BytesWritable(bytes);

    Assertions.assertArrayEquals(bytes, oi.getPrimitiveJavaObject(bytes));
    Assertions.assertEquals(bytesWritable, oi.getPrimitiveWritableObject(bytes));
    Assertions.assertEquals(bytes, oi.convert(bytes));

    byte[] copy = (byte[]) oi.copyObject(bytes);

    Assertions.assertArrayEquals(bytes, copy);
    Assertions.assertNotSame(bytes, copy);

    Assertions.assertFalse(oi.preferWritable());
  }

}

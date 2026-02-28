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

import java.util.UUID;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergUUIDObjectInspector {

  @Test
  public void testIcebergUUIDObjectInspector() {
    IcebergUUIDObjectInspector oi = IcebergUUIDObjectInspector.get();

    Assertions.assertEquals(ObjectInspector.Category.PRIMITIVE, oi.getCategory());
    Assertions.assertEquals(PrimitiveObjectInspector.PrimitiveCategory.STRING,
        oi.getPrimitiveCategory());

    Assertions.assertEquals(TypeInfoFactory.stringTypeInfo, oi.getTypeInfo());
    Assertions.assertEquals(TypeInfoFactory.stringTypeInfo.getTypeName(), oi.getTypeName());

    Assertions.assertEquals(String.class, oi.getJavaPrimitiveClass());
    Assertions.assertEquals(Text.class, oi.getPrimitiveWritableClass());

    Assertions.assertNull(oi.copyObject(null));
    Assertions.assertNull(oi.getPrimitiveJavaObject(null));
    Assertions.assertNull(oi.getPrimitiveWritableObject(null));
    Assertions.assertNull(oi.convert(null));

    UUID uuid = UUID.randomUUID();
    String uuidStr = uuid.toString();
    Text text = new Text(uuidStr);

    Assertions.assertEquals(uuidStr, oi.getPrimitiveJavaObject(text));
    Assertions.assertEquals(text, oi.getPrimitiveWritableObject(uuidStr));
    Assertions.assertEquals(uuid, oi.convert(uuidStr));

    Text copy = (Text) oi.copyObject(text);

    Assertions.assertEquals(text, copy);
    Assertions.assertNotSame(text, copy);

    Assertions.assertFalse(oi.preferWritable());
  }
}

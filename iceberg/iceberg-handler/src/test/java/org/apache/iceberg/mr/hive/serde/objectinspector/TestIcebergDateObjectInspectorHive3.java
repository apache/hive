/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.serde.objectinspector;

import java.time.LocalDate;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergDateObjectInspectorHive3 {

  @Test
  public void testIcebergDateObjectInspector() {
    DateObjectInspector oi = IcebergDateObjectInspectorHive3.get();

    Assertions.assertEquals(ObjectInspector.Category.PRIMITIVE, oi.getCategory());
    Assertions.assertEquals(PrimitiveObjectInspector.PrimitiveCategory.DATE,
        oi.getPrimitiveCategory());

    Assertions.assertEquals(TypeInfoFactory.dateTypeInfo, oi.getTypeInfo());
    Assertions.assertEquals(TypeInfoFactory.dateTypeInfo.getTypeName(), oi.getTypeName());

    Assertions.assertEquals(Date.class, oi.getJavaPrimitiveClass());
    Assertions.assertEquals(DateWritableV2.class, oi.getPrimitiveWritableClass());

    Assertions.assertNull(oi.copyObject(null));
    Assertions.assertNull(oi.getPrimitiveJavaObject(null));
    Assertions.assertNull(oi.getPrimitiveWritableObject(null));

    int epochDays = 5005;
    LocalDate local = LocalDate.ofEpochDay(epochDays);
    Date date = Date.ofEpochDay(epochDays);

    Assertions.assertEquals(date, oi.getPrimitiveJavaObject(local));
    Assertions.assertEquals(new DateWritableV2(date), oi.getPrimitiveWritableObject(local));

    Date copy = (Date) oi.copyObject(date);

    Assertions.assertEquals(date, copy);
    Assertions.assertNotSame(date, copy);

    Assertions.assertFalse(oi.preferWritable());
  }

}

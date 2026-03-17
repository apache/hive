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

import java.math.BigDecimal;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergDecimalObjectInspector {

  @Test
  public void testCache() {
    HiveDecimalObjectInspector oi = IcebergDecimalObjectInspector.get(38, 18);

    Assertions.assertSame(oi, IcebergDecimalObjectInspector.get(38, 18));
    Assertions.assertNotSame(oi, IcebergDecimalObjectInspector.get(28, 18));
    Assertions.assertNotSame(oi, IcebergDecimalObjectInspector.get(38, 28));
  }

  @Test
  public void testIcebergDecimalObjectInspector() {
    HiveDecimalObjectInspector oi = IcebergDecimalObjectInspector.get(38, 18);

    Assertions.assertEquals(ObjectInspector.Category.PRIMITIVE, oi.getCategory());
    Assertions.assertEquals(PrimitiveObjectInspector.PrimitiveCategory.DECIMAL,
        oi.getPrimitiveCategory());

    Assertions.assertEquals(new DecimalTypeInfo(38, 18), oi.getTypeInfo());
    Assertions.assertEquals(TypeInfoFactory.decimalTypeInfo.getTypeName(), oi.getTypeName());

    Assertions.assertEquals(38, oi.precision());
    Assertions.assertEquals(18, oi.scale());

    Assertions.assertEquals(HiveDecimal.class, oi.getJavaPrimitiveClass());
    Assertions.assertEquals(HiveDecimalWritable.class, oi.getPrimitiveWritableClass());

    Assertions.assertNull(oi.copyObject(null));
    Assertions.assertNull(oi.getPrimitiveJavaObject(null));
    Assertions.assertNull(oi.getPrimitiveWritableObject(null));

    HiveDecimal one = HiveDecimal.create(BigDecimal.ONE);

    Assertions.assertEquals(one, oi.getPrimitiveJavaObject(BigDecimal.ONE));
    Assertions.assertEquals(new HiveDecimalWritable(one),
        oi.getPrimitiveWritableObject(BigDecimal.ONE));

    HiveDecimal copy = (HiveDecimal) oi.copyObject(one);

    Assertions.assertEquals(one, copy);
    Assertions.assertNotSame(one, copy);

    Assertions.assertFalse(oi.preferWritable());
  }

}

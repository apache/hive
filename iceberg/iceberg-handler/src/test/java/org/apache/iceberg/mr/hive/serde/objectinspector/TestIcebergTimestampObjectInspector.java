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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergTimestampObjectInspector {

  @Test
  public void testIcebergTimestampObjectInspector() {
    IcebergTimestampObjectInspector oi = IcebergTimestampObjectInspector.get();

    Assertions.assertEquals(ObjectInspector.Category.PRIMITIVE, oi.getCategory());
    Assertions.assertEquals(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP,
        oi.getPrimitiveCategory());

    Assertions.assertEquals(TypeInfoFactory.timestampTypeInfo, oi.getTypeInfo());
    Assertions.assertEquals(TypeInfoFactory.timestampTypeInfo.getTypeName(), oi.getTypeName());

    Assertions.assertEquals(Timestamp.class, oi.getJavaPrimitiveClass());
    Assertions.assertEquals(TimestampWritableV2.class, oi.getPrimitiveWritableClass());

    Assertions.assertNull(oi.copyObject(null));
    Assertions.assertNull(oi.getPrimitiveJavaObject(null));
    Assertions.assertNull(oi.getPrimitiveWritableObject(null));
    Assertions.assertNull(oi.convert(null));

    long epochMilli = 1601471970000L;
    LocalDateTime local = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneId.of("UTC")).plusNanos(34000);
    Timestamp ts = Timestamp.ofEpochMilli(epochMilli);
    ts.setNanos(34000);

    Assertions.assertEquals(ts, oi.getPrimitiveJavaObject(local));
    Assertions.assertEquals(new TimestampWritableV2(ts), oi.getPrimitiveWritableObject(local));

    Timestamp copy = (Timestamp) oi.copyObject(ts);

    Assertions.assertEquals(ts, copy);
    Assertions.assertNotSame(ts, copy);

    Assertions.assertFalse(oi.preferWritable());

    Assertions.assertEquals(local, oi.convert(ts));
  }

}

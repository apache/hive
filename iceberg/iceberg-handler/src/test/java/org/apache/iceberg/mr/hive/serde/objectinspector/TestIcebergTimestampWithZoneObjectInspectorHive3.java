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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.mr.hive.serde.objectinspector;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergTimestampWithZoneObjectInspectorHive3 {

  @Test
  public void testIcebergTimestampLocalTZObjectInspector() {
    IcebergTimestampWithZoneObjectInspectorHive3 oi = IcebergTimestampWithZoneObjectInspectorHive3.get();

    Assertions.assertEquals(ObjectInspector.Category.PRIMITIVE, oi.getCategory());
    Assertions.assertEquals(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMPLOCALTZ,
        oi.getPrimitiveCategory());

    Assertions.assertEquals(TypeInfoFactory.timestampLocalTZTypeInfo, oi.getTypeInfo());
    Assertions.assertEquals(TypeInfoFactory.timestampLocalTZTypeInfo.getTypeName(), oi.getTypeName());

    Assertions.assertEquals(TimestampTZ.class, oi.getJavaPrimitiveClass());
    Assertions.assertEquals(TimestampLocalTZWritable.class, oi.getPrimitiveWritableClass());

    Assertions.assertNull(oi.copyObject(null));
    Assertions.assertNull(oi.getPrimitiveJavaObject(null));
    Assertions.assertNull(oi.getPrimitiveWritableObject(null));
    Assertions.assertNull(oi.convert(null));

    LocalDateTime dateTimeAtUTC = LocalDateTime.of(2020, 12, 10, 15, 55, 20, 30000);
    OffsetDateTime offsetDateTime = OffsetDateTime.of(dateTimeAtUTC.plusHours(4), ZoneOffset.ofHours(4));
    ZonedDateTime zdt = offsetDateTime.atZoneSameInstant(TypeInfoFactory.timestampLocalTZTypeInfo.getTimeZone());
    TimestampTZ ts = new TimestampTZ(dateTimeAtUTC.atZone(ZoneId.of("UTC")));

    Assertions.assertEquals(ts, oi.getPrimitiveJavaObject(offsetDateTime));
    Assertions.assertEquals(zdt, oi.getPrimitiveJavaObject(offsetDateTime).getZonedDateTime());
    Assertions.assertEquals(new TimestampLocalTZWritable(ts),
        oi.getPrimitiveWritableObject(offsetDateTime));

    // try with another offset as well
    offsetDateTime = OffsetDateTime.of(dateTimeAtUTC.plusHours(11), ZoneOffset.ofHours(11));
    Assertions.assertEquals(ts, oi.getPrimitiveJavaObject(offsetDateTime));
    Assertions.assertEquals(new TimestampLocalTZWritable(ts),
        oi.getPrimitiveWritableObject(offsetDateTime));

    TimestampTZ copy = (TimestampTZ) oi.copyObject(ts);

    Assertions.assertEquals(ts, copy);
    Assertions.assertNotSame(ts, copy);

    Assertions.assertFalse(oi.preferWritable());

    Assertions.assertEquals(OffsetDateTime.of(dateTimeAtUTC, ZoneOffset.UTC), oi.convert(ts));
  }

}

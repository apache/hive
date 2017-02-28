/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.convert;

import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetTableUtils;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Writable;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

public class TestETypeConverter {

  private ConverterParentHelper parent;
  private Timestamp ts;

  @Before
  public void init() {
    parent = new ConverterParentHelper();
    ts = Timestamp.valueOf("2011-01-01 01:01:01.111111111");
  }
  /**
   * This class helps to compare a Writable value pushed to the ConverterParent class.
   */
  private class ConverterParentHelper implements ConverterParent {
    private Writable value;
    private Map<String, String> metadata = new HashMap<>();

    /**
     * The set() method is called from within addXXXX() PrimitiveConverter methods.
     */
    @Override
    public void set(int index, Writable value) {
      this.value = value;
    }

    @Override
    public Map<String, String> getMetadata() {
      return metadata;
    }

    public void assertWritableValue(Writable expected) {
      assertEquals(expected.getClass(), value.getClass());
      assertEquals("Writable value set to Parent is different than expected", expected, value);
    }
  }

  private PrimitiveConverter getETypeConverter(ConverterParent parent, PrimitiveTypeName typeName, TypeInfo type) {
    return ETypeConverter.getNewConverter(new PrimitiveType(Type.Repetition.REQUIRED, typeName, "field"), 0, parent, type);
  }

  @Test
  public void testTimestampInt96ConverterLocal() {
   PrimitiveConverter converter;

    // Default timezone should be Localtime
    converter = getETypeConverter(parent, PrimitiveTypeName.INT96, TypeInfoFactory.timestampTypeInfo);
    converter.addBinary(NanoTimeUtils.getNanoTime(ts, Calendar.getInstance()).toBinary());
    parent.assertWritableValue(new TimestampWritable(ts));
  }

  @Test
  public void testTimestampInt96ConverterGMT() {
    PrimitiveConverter converter;

    parent.metadata.put(ParquetTableUtils.PARQUET_INT96_WRITE_ZONE_PROPERTY, "GMT");
    converter = getETypeConverter(parent, PrimitiveTypeName.INT96, TypeInfoFactory.timestampTypeInfo);
    converter.addBinary(NanoTimeUtils.getNanoTime(ts,
        Calendar.getInstance(TimeZone.getTimeZone("GMT"))).toBinary());
    parent.assertWritableValue(new TimestampWritable(ts));

  }

  @Test
  public void testTimestampInt96ConverterChicago() {
    PrimitiveConverter converter;

    parent.metadata.put(ParquetTableUtils.PARQUET_INT96_WRITE_ZONE_PROPERTY, "America/Chicago");
    converter = getETypeConverter(parent, PrimitiveTypeName.INT96, TypeInfoFactory.timestampTypeInfo);
    converter.addBinary(NanoTimeUtils.getNanoTime(ts,
        Calendar.getInstance(TimeZone.getTimeZone("America/Chicago"))).toBinary());
    parent.assertWritableValue(new TimestampWritable(ts));
  }

  @Test
  public void testTimestampInt96ConverterEtc() {
    PrimitiveConverter converter;

    parent.metadata.put(ParquetTableUtils.PARQUET_INT96_WRITE_ZONE_PROPERTY, "Etc/GMT-12");
    converter = getETypeConverter(parent, PrimitiveTypeName.INT96, TypeInfoFactory.timestampTypeInfo);
    converter.addBinary(NanoTimeUtils.getNanoTime(ts,
        Calendar.getInstance(TimeZone.getTimeZone("Etc/GMT-12"))).toBinary());
    parent.assertWritableValue(new TimestampWritable(ts));
  }
}

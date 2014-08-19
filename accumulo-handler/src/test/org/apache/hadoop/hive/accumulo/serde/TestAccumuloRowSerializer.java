/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.serde;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Joiner;

/**
 *
 */
public class TestAccumuloRowSerializer {

  @Test
  public void testBufferResetBeforeUse() throws IOException {
    ByteStream.Output output = new ByteStream.Output();
    PrimitiveObjectInspector fieldObjectInspector = Mockito.mock(StringObjectInspector.class);
    ColumnMapping mapping = Mockito.mock(ColumnMapping.class);

    // Write some garbage to the buffer that should be erased
    output.write("foobar".getBytes());

    // Stub out the serializer
    AccumuloRowSerializer serializer = Mockito.mock(AccumuloRowSerializer.class);

    String object = "hello";

    Mockito.when(
        serializer.getSerializedValue(Mockito.any(ObjectInspector.class), Mockito.any(),
            Mockito.any(ByteStream.Output.class), Mockito.any(ColumnMapping.class)))
        .thenCallRealMethod();

    Mockito.when(fieldObjectInspector.getCategory()).thenReturn(ObjectInspector.Category.PRIMITIVE);
    Mockito.when(fieldObjectInspector.getPrimitiveCategory()).thenReturn(PrimitiveCategory.STRING);
    Mockito.when(fieldObjectInspector.getPrimitiveWritableObject(Mockito.any(Object.class)))
        .thenReturn(new Text(object));
    Mockito.when(mapping.getEncoding()).thenReturn(ColumnEncoding.STRING);

    // Invoke the method
    serializer.getSerializedValue(fieldObjectInspector, object, output, mapping);

    // Verify the buffer was reset (real output doesn't happen because it was mocked)
    Assert.assertEquals(0, output.size());
  }

  @Test
  public void testBinarySerialization() throws IOException, SerDeException {
    List<String> columns = Arrays.asList("row", "cq1", "cq2", "cq3");
    List<TypeInfo> types = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.intTypeInfo, TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo);
    List<String> typeNames = new ArrayList<String>(types.size());
    for (TypeInfo type : types) {
      typeNames.add(type.getTypeName());
    }

    Properties tableProperties = new Properties();
    tableProperties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS,
        ":rowid,cf:cq1#b,cf:cq2#b,cf:cq3");
    tableProperties.setProperty(serdeConstants.FIELD_DELIM, " ");
    tableProperties.setProperty(serdeConstants.LIST_COLUMNS, Joiner.on(',').join(columns));
    tableProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(',').join(typeNames));
    AccumuloSerDeParameters accumuloSerDeParams = new AccumuloSerDeParameters(new Configuration(),
        tableProperties, AccumuloSerDe.class.getSimpleName());
    SerDeParameters serDeParams = accumuloSerDeParams.getSerDeParameters();

    LazySimpleStructObjectInspector oi = (LazySimpleStructObjectInspector) LazyFactory
        .createLazyStructInspector(columns, types, serDeParams.getSeparators(),
            serDeParams.getNullSequence(), serDeParams.isLastColumnTakesRest(),
            serDeParams.isEscaped(), serDeParams.getEscapeChar());

    AccumuloRowSerializer serializer = new AccumuloRowSerializer(0, serDeParams,
        accumuloSerDeParams.getColumnMappings(), new ColumnVisibility(),
        accumuloSerDeParams.getRowIdFactory());

    // Create the LazyStruct from the LazyStruct...Inspector
    LazyStruct obj = (LazyStruct) LazyFactory.createLazyObject(oi);

    ByteArrayRef byteRef = new ByteArrayRef();
    byteRef.setData(new byte[] {'r', 'o', 'w', '1', ' ', '1', '0', ' ', '2', '0', ' ', 'v', 'a',
        'l', 'u', 'e'});
    obj.init(byteRef, 0, byteRef.getData().length);

    Mutation m = (Mutation) serializer.serialize(obj, oi);

    Assert.assertArrayEquals("row1".getBytes(), m.getRow());

    List<ColumnUpdate> updates = m.getUpdates();
    Assert.assertEquals(3, updates.size());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    ColumnUpdate update = updates.get(0);
    Assert.assertEquals("cf", new String(update.getColumnFamily()));
    Assert.assertEquals("cq1", new String(update.getColumnQualifier()));

    out.writeInt(10);
    Assert.assertArrayEquals(baos.toByteArray(), update.getValue());

    update = updates.get(1);
    Assert.assertEquals("cf", new String(update.getColumnFamily()));
    Assert.assertEquals("cq2", new String(update.getColumnQualifier()));

    baos.reset();
    out.writeInt(20);
    Assert.assertArrayEquals(baos.toByteArray(), update.getValue());

    update = updates.get(2);
    Assert.assertEquals("cf", new String(update.getColumnFamily()));
    Assert.assertEquals("cq3", new String(update.getColumnQualifier()));

    Assert.assertEquals("value", new String(update.getValue()));
  }

  @Test
  public void testVisibilityLabel() throws IOException, SerDeException {
    List<String> columns = Arrays.asList("row", "cq1", "cq2", "cq3");
    List<TypeInfo> types = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.intTypeInfo, TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo);
    List<String> typeNames = new ArrayList<String>(types.size());
    for (TypeInfo type : types) {
      typeNames.add(type.getTypeName());
    }

    Properties tableProperties = new Properties();
    tableProperties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS,
        ":rowid,cf:cq1#b,cf:cq2#b,cf:cq3");
    tableProperties.setProperty(serdeConstants.FIELD_DELIM, " ");
    tableProperties.setProperty(serdeConstants.LIST_COLUMNS, Joiner.on(',').join(columns));
    tableProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(',').join(typeNames));
    AccumuloSerDeParameters accumuloSerDeParams = new AccumuloSerDeParameters(new Configuration(),
        tableProperties, AccumuloSerDe.class.getSimpleName());
    SerDeParameters serDeParams = accumuloSerDeParams.getSerDeParameters();

    LazySimpleStructObjectInspector oi = (LazySimpleStructObjectInspector) LazyFactory
        .createLazyStructInspector(columns, types, serDeParams.getSeparators(),
            serDeParams.getNullSequence(), serDeParams.isLastColumnTakesRest(),
            serDeParams.isEscaped(), serDeParams.getEscapeChar());

    AccumuloRowSerializer serializer = new AccumuloRowSerializer(0, serDeParams,
        accumuloSerDeParams.getColumnMappings(), new ColumnVisibility("foo"),
        accumuloSerDeParams.getRowIdFactory());

    // Create the LazyStruct from the LazyStruct...Inspector
    LazyStruct obj = (LazyStruct) LazyFactory.createLazyObject(oi);

    ByteArrayRef byteRef = new ByteArrayRef();
    byteRef.setData(new byte[] {'r', 'o', 'w', '1', ' ', '1', '0', ' ', '2', '0', ' ', 'v', 'a',
        'l', 'u', 'e'});
    obj.init(byteRef, 0, byteRef.getData().length);

    Mutation m = (Mutation) serializer.serialize(obj, oi);

    Assert.assertArrayEquals("row1".getBytes(), m.getRow());

    List<ColumnUpdate> updates = m.getUpdates();
    Assert.assertEquals(3, updates.size());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    ColumnUpdate update = updates.get(0);
    Assert.assertEquals("cf", new String(update.getColumnFamily()));
    Assert.assertEquals("cq1", new String(update.getColumnQualifier()));
    Assert.assertEquals("foo", new String(update.getColumnVisibility()));

    out.writeInt(10);
    Assert.assertArrayEquals(baos.toByteArray(), update.getValue());

    update = updates.get(1);
    Assert.assertEquals("cf", new String(update.getColumnFamily()));
    Assert.assertEquals("cq2", new String(update.getColumnQualifier()));
    Assert.assertEquals("foo", new String(update.getColumnVisibility()));

    baos.reset();
    out.writeInt(20);
    Assert.assertArrayEquals(baos.toByteArray(), update.getValue());

    update = updates.get(2);
    Assert.assertEquals("cf", new String(update.getColumnFamily()));
    Assert.assertEquals("cq3", new String(update.getColumnQualifier()));
    Assert.assertEquals("foo", new String(update.getColumnVisibility()));

    Assert.assertEquals("value", new String(update.getValue()));
  }

  @Test
  public void testMapSerialization() throws IOException, SerDeException {
    List<String> columns = Arrays.asList("row", "col");
    List<TypeInfo> types = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo, TypeInfoFactory
        .getMapTypeInfo(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo));
    List<String> typeNames = new ArrayList<String>(types.size());
    for (TypeInfo type : types) {
      typeNames.add(type.getTypeName());
    }

    Properties tableProperties = new Properties();
    tableProperties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowid,cf:*");
    tableProperties.setProperty(serdeConstants.FIELD_DELIM, " ");
    tableProperties.setProperty(serdeConstants.COLLECTION_DELIM, ",");
    tableProperties.setProperty(serdeConstants.MAPKEY_DELIM, ":");
    tableProperties.setProperty(serdeConstants.LIST_COLUMNS, Joiner.on(',').join(columns));
    tableProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(',').join(typeNames));
    AccumuloSerDeParameters accumuloSerDeParams = new AccumuloSerDeParameters(new Configuration(),
        tableProperties, AccumuloSerDe.class.getSimpleName());
    SerDeParameters serDeParams = accumuloSerDeParams.getSerDeParameters();

    TypeInfo stringTypeInfo = TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME);
    LazyStringObjectInspector stringOI = (LazyStringObjectInspector) LazyFactory
        .createLazyObjectInspector(stringTypeInfo, new byte[] {0}, 0,
            serDeParams.getNullSequence(), serDeParams.isEscaped(), serDeParams.getEscapeChar());

    LazyMapObjectInspector mapOI = LazyObjectInspectorFactory.getLazySimpleMapObjectInspector(
        stringOI, stringOI, (byte) ',', (byte) ':', serDeParams.getNullSequence(),
        serDeParams.isEscaped(), serDeParams.getEscapeChar());

    LazySimpleStructObjectInspector structOI = (LazySimpleStructObjectInspector) LazyObjectInspectorFactory
        .getLazySimpleStructObjectInspector(columns, Arrays.asList(stringOI, mapOI), (byte) ' ',
            serDeParams.getNullSequence(), serDeParams.isLastColumnTakesRest(),
            serDeParams.isEscaped(), serDeParams.getEscapeChar());

    AccumuloRowSerializer serializer = new AccumuloRowSerializer(0, serDeParams,
        accumuloSerDeParams.getColumnMappings(), new ColumnVisibility(),
        accumuloSerDeParams.getRowIdFactory());

    // Create the LazyStruct from the LazyStruct...Inspector
    LazyStruct obj = (LazyStruct) LazyFactory.createLazyObject(structOI);

    ByteArrayRef byteRef = new ByteArrayRef();
    byteRef.setData("row1 cq1:10,cq2:20,cq3:value".getBytes());
    obj.init(byteRef, 0, byteRef.getData().length);

    Mutation m = (Mutation) serializer.serialize(obj, structOI);

    Assert.assertArrayEquals("row1".getBytes(), m.getRow());

    List<ColumnUpdate> updates = m.getUpdates();
    Assert.assertEquals(3, updates.size());

    ColumnUpdate update = updates.get(0);
    Assert.assertEquals("cf", new String(update.getColumnFamily()));
    Assert.assertEquals("cq1", new String(update.getColumnQualifier()));
    Assert.assertEquals("10", new String(update.getValue()));

    update = updates.get(1);
    Assert.assertEquals("cf", new String(update.getColumnFamily()));
    Assert.assertEquals("cq2", new String(update.getColumnQualifier()));
    Assert.assertEquals("20", new String(update.getValue()));

    update = updates.get(2);
    Assert.assertEquals("cf", new String(update.getColumnFamily()));
    Assert.assertEquals("cq3", new String(update.getColumnQualifier()));
    Assert.assertEquals("value", new String(update.getValue()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidRowIdOffset() throws SerDeException {
    ArrayList<ColumnMapping> mappings = new ArrayList<ColumnMapping>();

    // Should fail because of the -1
    new AccumuloRowSerializer(-1, null, mappings, new ColumnVisibility(), null);
  }
}

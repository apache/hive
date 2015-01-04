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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.AccumuloHiveRow;
import org.apache.hadoop.hive.accumulo.LazyAccumuloRow;
import org.apache.hadoop.hive.accumulo.columns.InvalidColumnMappingException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;

public class TestAccumuloSerDe {
  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(TestAccumuloSerDe.class);

  protected AccumuloSerDe serde;

  @Before
  public void setup() {
    serde = new AccumuloSerDe();
  }

  @Test(expected = TooManyHiveColumnsException.class)
  public void moreHiveColumnsThanAccumuloColumns() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();

    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "row,field1,field2,field3,field4");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,string,string,string,string");

    serde.initialize(conf, properties);
    serde.deserialize(new Text("fail"));
  }

  @Test(expected = TooManyAccumuloColumnsException.class)
  public void moreAccumuloColumnsThanHiveColumns() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();

    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:f1,cf:f2,cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "row,field1,field2");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,string,string");

    serde.initialize(conf, properties);
    serde.deserialize(new Text("fail"));
  }

  @Test(expected = NullPointerException.class)
  public void emptyConfiguration() throws SerDeException {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    serde.initialize(conf, properties);
  }

  @Test
  public void simpleColumnMapping() throws SerDeException {
    Properties properties = new Properties();
    Configuration conf = new Configuration();

    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:f1,cf:f2,cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "row,field1,field2,field3");

    serde.initialize(conf, properties);
    assertNotNull(serde.getCachedRow());
  }

  @Test
  public void withRowID() throws SerDeException {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f1,:rowID,cf:f2,cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2,field3,field4");
    serde.initialize(conf, properties);
    assertNotNull(serde.getCachedRow());
  }

  @Test(expected = InvalidColumnMappingException.class)
  public void invalidColMapping() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf,cf:f2,cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field2,field3,field4");

    serde.initialize(conf, properties);
    AccumuloHiveRow row = new AccumuloHiveRow();
    row.setRowId("r1");
    Object obj = serde.deserialize(row);
    assertTrue(obj instanceof LazyAccumuloRow);
    LazyAccumuloRow lazyRow = (LazyAccumuloRow) obj;
    lazyRow.getField(0);
  }

  @Test(expected = TooManyAccumuloColumnsException.class)
  public void deserializeWithTooFewHiveColumns() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:f1,cf:f2,cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "row,col1,col2");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,string,string");

    serde.initialize(conf, properties);
    serde.deserialize(new Text("fail"));
  }

  @Test
  public void testArraySerialization() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();

    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:vals");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "row,values");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,array<string>");
    properties.setProperty(serdeConstants.COLLECTION_DELIM, ":");

    // Get one of the default separators to avoid having to set a custom separator
    char separator = ':';

    serde.initialize(conf, properties);

    AccumuloHiveRow row = new AccumuloHiveRow();
    row.setRowId("r1");
    row.add("cf", "vals", ("value1" + separator + "value2" + separator + "value3").getBytes());

    Object obj = serde.deserialize(row);

    assertNotNull(obj);
    assertTrue(obj instanceof LazyAccumuloRow);

    LazyAccumuloRow lazyRow = (LazyAccumuloRow) obj;
    Object field0 = lazyRow.getField(0);
    assertNotNull(field0);
    assertTrue(field0 instanceof LazyString);
    assertEquals(row.getRowId(), ((LazyString) field0).getWritableObject().toString());

    Object field1 = lazyRow.getField(1);
    assertNotNull(field1);
    assertTrue(field1 instanceof LazyArray);
    LazyArray array = (LazyArray) field1;

    List<Object> values = array.getList();
    assertEquals(3, values.size());
    for (int i = 0; i < 3; i++) {
      Object o = values.get(i);
      assertNotNull(o);
      assertTrue(o instanceof LazyString);
      assertEquals("value" + (i + 1), ((LazyString) o).getWritableObject().toString());
    }
  }

  @Test
  public void testMapSerialization() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();

    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:vals");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "row,values");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,map<string,string>");
    properties.setProperty(serdeConstants.COLLECTION_DELIM, ":");
    properties.setProperty(serdeConstants.MAPKEY_DELIM, "=");

    // Get one of the default separators to avoid having to set a custom separator
    char collectionSeparator = ':', kvSeparator = '=';

    serde.initialize(conf, properties);

    AccumuloHiveRow row = new AccumuloHiveRow();
    row.setRowId("r1");
    row.add("cf", "vals", ("k1" + kvSeparator + "v1" + collectionSeparator + "k2" + kvSeparator
        + "v2" + collectionSeparator + "k3" + kvSeparator + "v3").getBytes());

    Object obj = serde.deserialize(row);

    assertNotNull(obj);
    assertTrue(obj instanceof LazyAccumuloRow);

    LazyAccumuloRow lazyRow = (LazyAccumuloRow) obj;
    Object field0 = lazyRow.getField(0);
    assertNotNull(field0);
    assertTrue(field0 instanceof LazyString);
    assertEquals(row.getRowId(), ((LazyString) field0).getWritableObject().toString());

    Object field1 = lazyRow.getField(1);
    assertNotNull(field1);
    assertTrue(field1 instanceof LazyMap);
    LazyMap map = (LazyMap) field1;

    Map<Object,Object> untypedMap = map.getMap();
    assertEquals(3, map.getMapSize());
    Set<String> expectedKeys = new HashSet<String>();
    expectedKeys.add("k1");
    expectedKeys.add("k2");
    expectedKeys.add("k3");
    for (Entry<Object,Object> entry : untypedMap.entrySet()) {
      assertNotNull(entry.getKey());
      assertTrue(entry.getKey() instanceof LazyString);
      LazyString key = (LazyString) entry.getKey();

      assertNotNull(entry.getValue());
      assertTrue(entry.getValue() instanceof LazyString);
      LazyString value = (LazyString) entry.getValue();

      String strKey = key.getWritableObject().toString(), strValue = value.getWritableObject()
          .toString();

      assertTrue(expectedKeys.remove(strKey));

      assertEquals(2, strValue.length());
      assertTrue(strValue.startsWith("v"));
      assertTrue(strValue.endsWith(Character.toString(strKey.charAt(1))));
    }

    assertTrue("Did not find expected keys: " + expectedKeys, expectedKeys.isEmpty());
  }

  @Test
  public void deserialization() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:f1,cf:f2,cf:f3");

    properties.setProperty(serdeConstants.LIST_COLUMNS, "blah,field2,field3,field4");
    serde.initialize(conf, properties);

    AccumuloHiveRow row = new AccumuloHiveRow();
    row.setRowId("r1");
    row.add("cf", "f1", "v1".getBytes());
    row.add("cf", "f2", "v2".getBytes());

    Object obj = serde.deserialize(row);
    assertTrue(obj instanceof LazyAccumuloRow);

    LazyAccumuloRow lazyRow = (LazyAccumuloRow) obj;
    Object field0 = lazyRow.getField(0);
    assertNotNull(field0);
    assertTrue(field0 instanceof LazyString);
    assertEquals(field0.toString(), "r1");

    Object field1 = lazyRow.getField(1);
    assertNotNull(field1);
    assertTrue("Expected instance of LazyString but was " + field1.getClass(),
        field1 instanceof LazyString);
    assertEquals(field1.toString(), "v1");

    Object field2 = lazyRow.getField(2);
    assertNotNull(field2);
    assertTrue(field2 instanceof LazyString);
    assertEquals(field2.toString(), "v2");
  }

  @Test
  public void testNoVisibilitySetsEmptyVisibility() throws SerDeException {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f1,:rowID");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2");

    serde.initialize(conf, properties);

    AccumuloRowSerializer serializer = serde.getSerializer();

    Assert.assertEquals(new ColumnVisibility(), serializer.getVisibility());
  }

  @Test
  public void testColumnVisibilityForSerializer() throws SerDeException {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f1,:rowID");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2");
    properties.setProperty(AccumuloSerDeParameters.VISIBILITY_LABEL_KEY, "foobar");

    serde.initialize(conf, properties);

    AccumuloRowSerializer serializer = serde.getSerializer();

    Assert.assertEquals(new ColumnVisibility("foobar"), serializer.getVisibility());
  }

  @Test
  public void testCompositeKeyDeserialization() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:f1");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "row,field1");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "struct<col1:string,col2:string,col3:string>,string");
    properties.setProperty(DelimitedAccumuloRowIdFactory.ACCUMULO_COMPOSITE_DELIMITER, "_");
    properties.setProperty(AccumuloSerDeParameters.COMPOSITE_ROWID_FACTORY,
        DelimitedAccumuloRowIdFactory.class.getName());

    serde.initialize(conf, properties);

    AccumuloHiveRow row = new AccumuloHiveRow();
    row.setRowId("p1_p2_p3");
    row.add("cf", "f1", "v1".getBytes());

    Object obj = serde.deserialize(row);
    assertTrue(obj instanceof LazyAccumuloRow);

    LazyAccumuloRow lazyRow = (LazyAccumuloRow) obj;
    Object field0 = lazyRow.getField(0);
    assertNotNull(field0);
    assertTrue(field0 instanceof LazyStruct);
    LazyStruct struct = (LazyStruct) field0;
    List<Object> fields = struct.getFieldsAsList();
    assertEquals(3, fields.size());
    for (int i = 0; i < fields.size(); i++) {
      assertEquals(LazyString.class, fields.get(i).getClass());
      assertEquals("p" + (i + 1), fields.get(i).toString());
    }

    Object field1 = lazyRow.getField(1);
    assertNotNull(field1);
    assertTrue("Expected instance of LazyString but was " + field1.getClass(),
        field1 instanceof LazyString);
    assertEquals(field1.toString(), "v1");
  }

  @Test
  public void testStructOfMapSerialization() throws IOException, SerDeException {
    List<String> columns = Arrays.asList("row", "col");
    List<String> structColNames = Arrays.asList("map1", "map2");
    TypeInfo mapTypeInfo = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo);

    // struct<map1:map<string,string>,map2:map<string,string>>,string
    List<TypeInfo> types = Arrays.<TypeInfo> asList(
        TypeInfoFactory.getStructTypeInfo(structColNames, Arrays.asList(mapTypeInfo, mapTypeInfo)),
        TypeInfoFactory.stringTypeInfo);

    Properties tableProperties = new Properties();
    tableProperties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowid,cf:cq");
    // Use the default separators [0, 1, 2, 3, ..., 7]
    tableProperties.setProperty(serdeConstants.LIST_COLUMNS, Joiner.on(',').join(columns));
    tableProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(',').join(types));
    AccumuloSerDeParameters accumuloSerDeParams = new AccumuloSerDeParameters(new Configuration(),
        tableProperties, AccumuloSerDe.class.getSimpleName());
    SerDeParameters serDeParams = accumuloSerDeParams.getSerDeParameters();

    byte[] seps = serDeParams.getSeparators();

    // struct<map<k:v,k:v>_map<k:v,k:v>>>

    TypeInfo stringTypeInfo = TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME);
    LazyStringObjectInspector stringOI = (LazyStringObjectInspector) LazyFactory
        .createLazyObjectInspector(stringTypeInfo, new byte[] {0}, 0,
            serDeParams.getNullSequence(), serDeParams.isEscaped(), serDeParams.getEscapeChar());

    LazyMapObjectInspector mapOI = LazyObjectInspectorFactory.getLazySimpleMapObjectInspector(
        stringOI, stringOI, seps[3], seps[4], serDeParams.getNullSequence(),
        serDeParams.isEscaped(), serDeParams.getEscapeChar());

    LazySimpleStructObjectInspector rowStructOI = (LazySimpleStructObjectInspector) LazyObjectInspectorFactory
        .getLazySimpleStructObjectInspector(structColNames,
            Arrays.<ObjectInspector> asList(mapOI, mapOI), (byte) seps[2],
            serDeParams.getNullSequence(), serDeParams.isLastColumnTakesRest(),
            serDeParams.isEscaped(), serDeParams.getEscapeChar());

    LazySimpleStructObjectInspector structOI = (LazySimpleStructObjectInspector) LazyObjectInspectorFactory
        .getLazySimpleStructObjectInspector(columns, Arrays.asList(rowStructOI, stringOI), seps[1],
            serDeParams.getNullSequence(), serDeParams.isLastColumnTakesRest(),
            serDeParams.isEscaped(), serDeParams.getEscapeChar());

    AccumuloRowSerializer serializer = new AccumuloRowSerializer(0, serDeParams,
        accumuloSerDeParams.getColumnMappings(), new ColumnVisibility(),
        accumuloSerDeParams.getRowIdFactory());

    Map<String,String> map1 = new HashMap<String,String>(), map2 = new HashMap<String,String>();

    map1.put("key10", "value10");
    map1.put("key11", "value11");

    map2.put("key20", "value20");
    map2.put("key21", "value21");

    ByteArrayRef byteRef = new ByteArrayRef();
    // Default separators are 1-indexed (instead of 0-indexed), thus the separator at offset 1 is
    // (byte) 2
    // The separator for the hive row is \x02, for the row Id struct, \x03, and the maps \x04 and
    // \x05
    String accumuloRow = "key10\5value10\4key11\5value11\3key20\5value20\4key21\5value21";
    LazyStruct entireStruct = (LazyStruct) LazyFactory.createLazyObject(structOI);
    byteRef.setData((accumuloRow + "\2foo").getBytes());
    entireStruct.init(byteRef, 0, byteRef.getData().length);

    Mutation m = serializer.serialize(entireStruct, structOI);
    Assert.assertArrayEquals(accumuloRow.getBytes(), m.getRow());
    Assert.assertEquals(1, m.getUpdates().size());
    ColumnUpdate update = m.getUpdates().get(0);
    Assert.assertEquals("cf", new String(update.getColumnFamily()));
    Assert.assertEquals("cq", new String(update.getColumnQualifier()));
    Assert.assertEquals("foo", new String(update.getValue()));

    AccumuloHiveRow haRow = new AccumuloHiveRow(new String(m.getRow()));
    haRow.add("cf", "cq", "foo".getBytes());

    LazyAccumuloRow lazyAccumuloRow = new LazyAccumuloRow(structOI);
    lazyAccumuloRow.init(haRow, accumuloSerDeParams.getColumnMappings(),
        accumuloSerDeParams.getRowIdFactory());

    List<Object> objects = lazyAccumuloRow.getFieldsAsList();
    Assert.assertEquals(2, objects.size());

    Assert.assertEquals("foo", objects.get(1).toString());

    LazyStruct rowStruct = (LazyStruct) objects.get(0);
    List<Object> rowObjects = rowStruct.getFieldsAsList();
    Assert.assertEquals(2, rowObjects.size());

    LazyMap rowMap = (LazyMap) rowObjects.get(0);
    Map<?,?> actualMap = rowMap.getMap();
    System.out.println("Actual map 1: " + actualMap);
    Map<String,String> actualStringMap = new HashMap<String,String>();
    for (Entry<?,?> entry : actualMap.entrySet()) {
      actualStringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }

    Assert.assertEquals(map1, actualStringMap);

    rowMap = (LazyMap) rowObjects.get(1);
    actualMap = rowMap.getMap();
    System.out.println("Actual map 2: " + actualMap);
    actualStringMap = new HashMap<String,String>();
    for (Entry<?,?> entry : actualMap.entrySet()) {
      actualStringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }

    Assert.assertEquals(map2, actualStringMap);
  }
}
